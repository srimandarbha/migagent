from pathlib import Path
from ..contracts import AgentState, Hypothesis, RecoveryOption, Readiness
from ..rules.classification import classify
from ..rules.evidence_policy import required_for, optional_for, adaptive_for, load_policy, hypotheses_for
from ..rules.safety_gates import gate_recovery, required_complete
from ..tools.investigation import InvestigationTool
from ..skills.loader import SkillRepository
from ..memory.context import MemoryContext
from ..llm import build_llm_provider
from ..llm.advisory import build_messages, parse_advisory
from ..llm.knowledge_judge import build_messages as build_judge_messages, parse_judge
from ..environment import resolve as resolve_environment
from ..knowledge_compatibility import resolve_applicable, judge_eligibility, apply_judge_verdict
from ..memory.recurrence import build_base_signature, build_evidence_signature, correlate as correlate_recurrence
from ..tools.coverage import calculate_coverage
from ..rules.learning import FIRST_SEEN, RECURRING_UNKNOWN, RECURRING_UNRESOLVED, RECURRING_RESOLVED, KNOWN_ISSUE

class MigrationFailureEngine:
    def __init__(self, registry, max_iterations=12, tracker=None, knowledge=None, skill_root=None, memory_mode='both', llm_provider=None, knowledge_store=None):
        self.registry=registry; self.max_iterations=max_iterations; self.tracker=tracker; self.knowledge=knowledge; self.memory_mode=memory_mode
        self.llm_provider = llm_provider if llm_provider is not None else build_llm_provider()
        self.skill_repo=SkillRepository(skill_root or Path(__file__).parents[2] / 'skills')
        from ..memory import DynamicKnowledgeStore, LearningPipeline
        self.knowledge_store = knowledge_store if knowledge_store is not None else DynamicKnowledgeStore(tracker=tracker)
        self.learning_pipeline = LearningPipeline(self.knowledge_store, tracker=tracker)
        self._graph = None
        self._graph_initialized = False

    def run(self, request):
        if not isinstance(request, dict):
            raise TypeError('Migration Failure Agent request must be a plain dictionary')
        graph = self._get_graph()
        if graph is None:
            # Development/test fallback when the declared LangGraph dependency is absent.
            # Do not catch graph execution errors and silently fall back.
            return self._run_legacy(request)
        # Explicit recursion_limit tied to max_iterations, not LangGraph's unrelated
        # default of 25. The evidence loop is a multi-node cycle per round, so an
        # untuned default could trip before our own deterministic bound does.
        result = graph.invoke(
            {"request": request},
            config={"recursion_limit": max(25, self.max_iterations * 4)},
        )
        raw = result["agent_state"]
        return raw if isinstance(raw, AgentState) else AgentState.from_dict(raw)

    def _run_legacy(self, request):
        if not isinstance(request, dict):
            raise TypeError('Migration Failure Agent request must be a plain dictionary')
        from .nodes.implementation import MigrationFailureNodes
        nodes = MigrationFailureNodes(self)
        g_state = {"request": request}
        g_state = nodes.create_case(g_state)
        g_state = nodes.classify(g_state)
        g_state = nodes.persist_case(g_state)
        g_state = nodes.load_context(g_state)
        g_state = nodes.load_environment(g_state)
        g_state = nodes.check_knowledge_compatibility(g_state)
        if nodes.route_after_knowledge_compatibility(g_state) == 'judge':
            g_state = nodes.judge_knowledge_applicability(g_state)
        g_state = nodes.correlate_recurrence(g_state)
        g_state = nodes.build_evidence_plan(g_state)

        # Adaptive evidence loop matching LangGraph graph routes
        while True:
            g_state = nodes.collect_evidence(g_state)
            g_state = nodes.evaluate_evidence(g_state)
            route = nodes.route_after_evidence(g_state)
            if route == "sufficient":
                g_state = nodes.collect_optional(g_state)
                break
            elif route == "investigate":
                g_state = nodes.plan_next_evidence(g_state)
                if nodes.route_after_plan(g_state) != "collect":
                    break
            else:
                break

        g_state = nodes.evaluate_hypotheses(g_state)
        g_state = nodes.diagnose(g_state)
        if nodes.route_after_diagnosis(g_state) == 'advisory':
            g_state = nodes.llm_advisory(g_state)
        g_state = nodes.calculate_readiness(g_state)
        g_state = nodes.recommend(g_state)
        g_state = nodes.persist(g_state)
        g_state = nodes.finalize(g_state)
        raw = g_state["agent_state"]
        return raw if isinstance(raw, AgentState) else AgentState.from_dict(raw)

    def _get_graph(self):
        if not self._graph_initialized:
            from .graph import build_graph
            self._graph = build_graph(self)
            self._graph_initialized = True
        return self._graph

    def _create_state(self, request):
        event=request.get('event', request)
        failure_case_id=request.get('failure_case_id', event.get('failure_case_id'))
        if not failure_case_id:
            failure_case_id=event.get('event_id') or request.get('incident_id')
        if not failure_case_id:
            raise ValueError('Migration Failure Agent requires failure_case_id')
        state=AgentState(failure_case_id=str(failure_case_id), event=event, success_conditions=['failure confirmed','failed phase identified','required evidence verified','diagnosis evidence-backed','next step safety-gated'])
        state.context={k:v for k,v in event.items() if k not in ('hidden_truth','truth','answer_key')}
        state.trace.append('context loaded')
        return state

    def _persist_case_event(self, state, request):
        event=state.event
        if not self.tracker:
            return
        try:
            case_id=self.tracker.create_or_get_failure_case(event_id=str(event.get('event_id', state.failure_case_id)), migration_id=str(event.get('migration_id','unknown')), vm_id=event.get('vm_id'), cluster_id=event.get('cluster_id'), change_id=event.get('change_id'), failure_class=state.classification, failure_code=state.classification.split('.')[-1] if state.classification else None, agent_version=state.agent_version, policy_version=state.policy_version, severity=event.get('severity'))
            state.failure_case_id=str(case_id)
            kafka=request.get('kafka', {})
            if kafka and hasattr(self.tracker, 'save_event'):
                from datetime import datetime, timezone
                self.tracker.save_event(event_id=str(event.get('event_id', state.failure_case_id)), failure_case_id=case_id, event_type=str(event.get('event_type','MigrationFailed')), event_time=datetime.fromisoformat(str(kafka.get('event_time')).replace('Z','+00:00')) if kafka.get('event_time') else datetime.now(timezone.utc), payload=event, kafka_topic=kafka.get('topic'), kafka_partition=kafka.get('partition'), kafka_offset=kafka.get('offset'))
        except Exception as exc:
            state.errors.append(f'tracker case/event persistence: {exc}')

    def _classify(self, state):
        msg = state.event.get('message', '') or state.event.get('error', '') or state.event.get('description', '')
        dynamic_matches = self.knowledge_store.match_failure(msg, state.context) if hasattr(self, 'knowledge_store') and self.knowledge_store else []
        state.classification,state.classification_confidence=classify(state.event)
        if (state.classification in (None, 'UNKNOWN', 'UNCLASSIFIED') or state.classification_confidence < 0.6) and dynamic_matches:
            top_sig, conf = dynamic_matches[0]
            state.classification = top_sig.mechanism
            state.classification_confidence = conf
            state.failure_signature = top_sig.signature_id
            state.trace.append(f'dynamic knowledge match signature={top_sig.signature_id} conf={conf}')
        elif dynamic_matches:
            state.failure_signature = dynamic_matches[0][0].signature_id
        state.trace.append(f'classify {state.classification}')
        policy=load_policy(state.classification)
        state.policy_version=policy.get('version')
        coverage = calculate_coverage(state.classification, self.registry)
        state.capability_coverage = coverage.to_dict()
        state.missing_diagnostic_capabilities = list(coverage.missing_required)
        state.trace.append(f"capability coverage {coverage.status} required={coverage.required_coverage}")
        state.skill_id=policy.get('skill')
        if state.skill_id and self.skill_repo.exists(state.skill_id):
            state.skill=self.skill_repo.load(state.skill_id)
            state.trace.append(f'skill loaded {state.skill_id}')
        else:
            state.errors.append(f'skill unavailable: {state.skill_id}')

    def _load_context(self, state, request):
        memory_mode=request.get('memory_mode', self.memory_mode)
        MemoryContext(self.tracker,self.knowledge).load(state, memory_mode)

    def _load_environment(self, state):
        state.environment = resolve_environment(self.registry, state.event)
        state.context['environment'] = state.environment.to_dict()
        state.trace.append('environment fingerprint loaded')

    def _judge_knowledge_applicability(self, state):
        """Use LLM only to adjudicate ambiguous compatibility candidates.

        Explicit version mismatches, retired/conflicting knowledge and explicit
        error mismatches are never sent to the LLM and cannot be overridden by it.
        """
        if self.llm_provider is None:
            return {'status': 'NOT_CONFIGURED', 'reason': 'No LLM provider configured.'}
        environment = state.environment.to_dict() if state.environment else {}
        failure = {
            'failure_code': state.event.get('failure_code') or state.event.get('error_code'),
            'classification': state.classification,
            'failure_class': state.event.get('failure_class'),
        }
        candidates = judge_eligibility(environment, failure, state.knowledge_candidates)
        if not candidates:
            return {'status': 'NOT_REQUESTED', 'reason': 'No ambiguous knowledge candidates require adjudication.'}

        results = []
        for candidate in candidates[:5]:
            try:
                import time
                started = time.monotonic()
                response = self.llm_provider.generate(
                    build_judge_messages(environment, failure, [candidate], state.historical_context[:10]),
                    temperature=0, max_tokens=700, response_format={'type': 'json_object'},
                )
                verdict = parse_judge(response)
                verdict.update({
                    'candidate_id': str(candidate.get('id', candidate.get('document_id', ''))),
                    'candidate_title': candidate.get('title'),
                    'latency_ms': round((time.monotonic() - started) * 1000, 1),
                    'provider': type(self.llm_provider).__name__,
                    'model': getattr(self.llm_provider, 'model', None),
                })
                apply_judge_verdict([candidate], verdict)
                results.append(verdict)
            except Exception as exc:
                results.append({
                    'status': 'UNAVAILABLE',
                    'candidate_id': str(candidate.get('id', candidate.get('document_id', ''))),
                    'reason': str(exc),
                    'provider': type(self.llm_provider).__name__,
                    'model': getattr(self.llm_provider, 'model', None),
                })

        # Store the advisory separately. It never becomes deterministic evidence.
        for candidate, verdict in zip(candidates[:len(results)], results):
            candidate['knowledge_judge'] = verdict
        return {
            'status': 'COMPLETED',
            'results': results,
            'hard_rejects_untouched': True,
            'policy': 'LLM_MAY_ADJUDICATE_UNKNOWN_ONLY',
        }

    def _check_knowledge_compatibility(self, state):
        state.knowledge_candidates = list(state.knowledge_context)
        environment = state.environment.to_dict() if state.environment else {}
        failure = {
            'failure_code': state.event.get('failure_code') or state.event.get('error_code'),
            'classification': state.classification,
            'failure_class': state.event.get('failure_class'),
        }
        compatibility = resolve_applicable(environment, state.knowledge_candidates, failure)
        state.applicable_knowledge = compatibility['supported']
        state.version_conflicts = compatibility['excluded']
        state.compatibility_context = {
            'supported_count': len(compatibility['supported']),
            'background_count': len(compatibility['background']),
            'excluded_count': len(compatibility['excluded']),
            'excluded_ids': [str(x.get('id', x.get('document_id', ''))) for x in compatibility['excluded']],
            'background_ids': [str(x.get('id', x.get('document_id', ''))) for x in compatibility['background']],
            'candidate_decisions': [{
                'id': str(x.get('id', x.get('document_id', ''))),
                'title': x.get('title'),
                'error_match': x.get('compatibility', {}).get('error_match'),
                'version_applicability': x.get('compatibility', {}).get('version_applicability'),
                'recommendation_status': x.get('compatibility', {}).get('recommendation_status'),
            } for x in compatibility['all']],
        }
        state.trace.append(f'knowledge compatibility supported={len(state.applicable_knowledge)} excluded={len(state.version_conflicts)}')

    def _correlate_recurrence(self, state):
        """Correlate the current failure against historical cases without diagnosing it.

        Absence of history/knowledge is a normal state. It becomes a knowledge-gap
        signal, not an agent error. Historical actions are never promoted to fixes
        without verified outcome + validation.
        """
        state.failure_signature = state.failure_signature or build_base_signature(state.event, state.classification)
        result = correlate_recurrence(
            self.tracker,
            signature=state.failure_signature,
            failure_case_id=state.failure_case_id,
            classification=state.classification,
            cluster_id=state.event.get('cluster_id'),
        )
        state.recurrence = result
        state.recurrence.setdefault('signature', state.failure_signature)

        has_dynamic_solution = False
        if hasattr(self, 'knowledge_store') and self.knowledge_store:
            sig = self.knowledge_store.get_signature(state.failure_signature)
            if not sig:
                msg = state.event.get('message', '') or state.event.get('error', '')
                matches = self.knowledge_store.match_failure(msg, state.context)
                if matches:
                    sig = matches[0][0]
            if sig and any(s.success_rate >= 0.7 for s in sig.solutions):
                has_dynamic_solution = True

        state.learning = {
            'status': 'NOT_EVALUATED',
            'resolution_recorded': bool(result.get('previous_outcomes')) or has_dynamic_solution,
            'validated_solution_exists': bool(result.get('validated_solution_refs')) or has_dynamic_solution,
            'knowledge_gap': (result.get('recurrence_status') in {FIRST_SEEN, RECURRING_UNKNOWN, RECURRING_UNRESOLVED}) and not has_dynamic_solution,
        }
        if has_dynamic_solution:
            state.learning.update({'status': 'VALIDATED_KNOWLEDGE_AVAILABLE', 'reason': 'A validated solution exists in the Dynamic Knowledge Store for this signature.'})
        elif result.get('status') == 'DISABLED':
            state.learning.update({'status': 'MEMORY_DISABLED', 'reason': 'SRE Tracker recurrence lookup is disabled for this run.'})
        elif result.get('status') == 'UNAVAILABLE':
            state.learning.update({'status': 'MEMORY_UNAVAILABLE', 'reason': 'SRE Tracker history is unavailable; recurrence cannot be established.'})
        elif result.get('status') == 'ERROR':
            state.learning.update({'status': 'MEMORY_ERROR', 'reason': result.get('error', 'SRE Tracker recurrence lookup failed.')})
        elif result.get('recurrence_status') == FIRST_SEEN and not state.historical_context and not state.applicable_knowledge:
            state.learning.update({'status': 'NEW_FAILURE', 'reason': 'No historical SRE resolution or applicable RHOKP knowledge is available.'})
        elif result.get('recurrence_status') in {RECURRING_UNKNOWN, RECURRING_UNRESOLVED}:
            state.learning.update({'status': 'KNOWLEDGE_GAP', 'reason': 'The failure recurs, but no validated reusable resolution exists.'})
        elif result.get('recurrence_status') == RECURRING_RESOLVED:
            state.learning.update({'status': 'RESOLVED_HISTORY_UNVALIDATED', 'reason': 'Previous occurrences were resolved, but no validated reusable solution exists.'})
        elif result.get('recurrence_status') == KNOWN_ISSUE:
            state.learning.update({'status': 'VALIDATED_KNOWLEDGE_AVAILABLE', 'reason': 'A validated solution exists for the matching failure signature.'})
        else:
            state.learning.update({'status': 'NO_PRIOR_MATCH'})
        if self.tracker and hasattr(self.tracker, 'update_case_metadata'):
            try:
                self.tracker.update_case_metadata(
                    state.failure_case_id,
                    failure_signature=state.failure_signature,
                    recurrence_status=result.get('recurrence_status'),
                    occurrence_number=result.get('occurrence_count'),
                )
            except Exception as exc:
                state.errors.append(f'recurrence metadata persistence: {exc}')
        state.trace.append(f"recurrence {result.get('recurrence_status')} occurrences={result.get('occurrence_count', 1)}")

    def record_resolution(self, *, failure_case_id, resolution_code, description,
                          outcome_status='RESOLVED', verification_status='PASSED',
                          recorded_by=None, validated_by=None, validation_reason=None,
                          action=None, expected_state=None, observed_state=None,
                          evidence_ids=None, environment_context=None):
        """Record an externally performed SRE action/outcome.

        This method never executes the action. It is the write-side contract used
        after the read-only diagnostic workflow has been acted on by an SRE.
        """
        from ..learning import LearningLifecycle
        case = self.tracker.get_failure_case(str(failure_case_id)) if hasattr(self.tracker, 'get_failure_case') else None
        if not case:
            raise KeyError(f'failure case not found: {failure_case_id}')
        res = LearningLifecycle(self.tracker).record_resolution(
            failure_case_id=str(failure_case_id), resolution_code=resolution_code,
            description=description, outcome_status=outcome_status,
            verification_status=verification_status, recorded_by=recorded_by,
            validated_by=validated_by, validation_reason=validation_reason,
            action=action, expected_state=expected_state, observed_state=observed_state,
            evidence_ids=evidence_ids, environment_context=environment_context or {},
            failure_signature=case.get('failure_signature'),
            failure_class=case.get('failure_class'), failure_code=case.get('failure_code'),
            diagnosis_code=case.get('diagnosis_code'),
        )
        if hasattr(self, 'learning_pipeline') and self.learning_pipeline:
            sig_id = case.get('failure_signature')
            if sig_id:
                success = (outcome_status == 'RESOLVED' and verification_status == 'PASSED')
                self.learning_pipeline.record_action_outcome(sig_id, success=success)
        return res

    def promote_learning_candidate(self, candidate_id: str, validated_by: str, validation_reason: str, **kwargs):
        """Promote an operational learning candidate to an active FailureSignature in the Dynamic Knowledge Store."""
        if not hasattr(self, 'learning_pipeline') or not self.learning_pipeline:
            raise RuntimeError("Learning pipeline is not available on this engine instance.")
        return self.learning_pipeline.promote_candidate(
            candidate_id=candidate_id,
            validated_by=validated_by,
            validation_reason=validation_reason,
            **kwargs,
        )

    def _gate_recovery(self, state, action):
        return gate_recovery(state, action)

    def _persist_results(self, state):
        if not self.tracker:
            return
        try:
            ids=[]
            for e in state.evidence:
                try:
                    ids.append(self.tracker.save_evidence(state.failure_case_id,e.__dict__))
                except Exception as exc:
                    state.errors.append(f'tracker evidence persistence: {exc}')
            state.diagnosis.setdefault('evidence_ids',[str(x) for x in ids])
            facts=[e.fact for e in state.evidence if e.status.value=='SUCCESS']
            state.evidence_signature=build_evidence_signature(state.classification, state.diagnosis.get('mechanism'), facts)
            if hasattr(self.tracker, 'update_case_metadata'):
                self.tracker.update_case_metadata(
                    state.failure_case_id,
                    failure_signature=state.failure_signature,
                    evidence_signature=state.evidence_signature,
                    recurrence_status=state.recurrence.get('recurrence_status'),
                    occurrence_number=state.recurrence.get('occurrence_count'),
                )
            self.tracker.save_diagnosis(state.failure_case_id,state.diagnosis,ids)
            if hasattr(self.tracker, 'mark_event_completed'):
                self.tracker.mark_event_completed(state.event.get('event_id', state.failure_case_id))
        except Exception as exc:
            state.errors.append(f'tracker diagnosis persistence: {exc}')

    def _initialize_evidence_plan(self, state):
        plan=[]
        for req in required_for(state.classification):
            plan.append({**req,'purpose':'Required evidence','status':'PENDING','round':0})
        state.evidence_plan=plan

    def _collect_round(self, state, tool, required_only=False, requirements=None):
        reqs=requirements if requirements is not None else required_for(state.classification)
        for req in reqs:
            if state.iteration >= self.max_iterations: break
            key=self._req_key(req)
            if any(r.get('capability')==req.get('capability') and r.get('parameters')==req.get('parameters') and r.get('status') in ('SUCCESS','NO_DATA','UNKNOWN','UNAVAILABLE','ERROR') for r in state.evidence_plan):
                # Existing plan entries are updated below; duplicate capability requests are avoided.
                existing=[r for r in state.evidence_plan if r.get('capability')==req.get('capability') and r.get('parameters')==req.get('parameters')]
                if existing and existing[0].get('status') not in ('PENDING','RETRY') and requirements is not None:
                    continue
            result_before=len(state.capability_results)
            tool.collect(state,req); state.iteration+=1; state.attempt_count+=1
            result=state.capability_results[-1] if len(state.capability_results)>result_before else None
            for plan_item in state.evidence_plan:
                if self._req_key(plan_item)==key:
                    plan_item['status']=result.get('status') if result else 'ERROR'; plan_item['round']=state.evidence_round; break
            else:
                state.evidence_plan.append({**req,'purpose':req.get('purpose','Adaptive evidence'),'status':result.get('status') if result else 'ERROR','round':state.evidence_round})

    @staticmethod
    def _req_key(req):
        p=req.get('parameters',{})
        return (req.get('capability'),p.get('domain'),p.get('signal'))

    def _evaluate_evidence(self,state):
        required_ok, required_missing = required_complete(state)
        coverage = state.capability_coverage or {}
        missing_capabilities = list(coverage.get('missing_required', []))
        if coverage.get('required_coverage') == 'BLOCKED' and missing_capabilities:
            required_ok = False
            required_missing = list(dict.fromkeys([*required_missing, *missing_capabilities]))
        failed=[r for r in state.capability_results if r.get('status') in {'NO_DATA','UNKNOWN','UNAVAILABLE','ERROR','NOT_REGISTERED'}]
        diagnosis_missing=[]
        facts={e.fact for e in state.evidence if e.status.value=='SUCCESS'}
        adaptive=adaptive_for(state.classification)
        for rule in adaptive.get('rules',[]) if adaptive.get('enabled') else []:
            when=set(rule.get('when',{}).get('all_facts',[]))
            any_when=set(rule.get('when',{}).get('any_facts',[]))
            unless=set(rule.get('unless_any_facts',[]))
            matches=(when.issubset(facts) if when else True) and (bool(any_when & facts) if any_when else True)
            if matches and not (unless & facts):
                for req in rule.get('investigate',[]):
                    key=self._req_key(req)
                    if not any(self._req_key(r)==key and r.get('status') in {'SUCCESS','NO_DATA','UNKNOWN','UNAVAILABLE','ERROR'} for r in state.evidence_plan):
                        diagnosis_missing.append({**req,'rule_id':rule.get('id'),'purpose':req.get('purpose','Targeted investigation')})
        successful_count=sum(1 for e in state.evidence if e.status.value=='SUCCESS')
        if state.classification == 'UNKNOWN' or not required_ok or successful_count == 0:
            status='BLOCKED' if failed else 'INSUFFICIENT'
            diagnosis_status='INSUFFICIENT'
        elif diagnosis_missing:
            status='INSUFFICIENT'
            diagnosis_status='INSUFFICIENT'
        elif failed and state.evidence_round>0:
            status='BLOCKED'
            diagnosis_status='INSUFFICIENT'
        else:
            status='SUFFICIENT'
            diagnosis_status='SUFFICIENT'
        if failed:
            collection_status='BLOCKED'
        elif required_ok and not diagnosis_missing and successful_count > 0:
            collection_status='COMPLETE'
        else:
            collection_status='INCOMPLETE'
        return {'status':status,'diagnosis_status':diagnosis_status,'collection_status':collection_status,'required_missing':list(required_missing),'missing_evidence':diagnosis_missing,'failed_capabilities':failed,'successful_evidence_count':successful_count,'round':state.evidence_round}

    def _select_next_evidence(self,state):
        return list(state.evidence_evaluation.get('missing_evidence',[]))

    def _hypotheses(self, state):
        evidence_by_fact: dict[str, list] = {}
        for e in state.evidence:
            if e.status.value == 'SUCCESS':
                evidence_by_fact.setdefault(e.fact, []).append(e)

        from ..rules.hypothesis_scorer import evaluate_hypothesis_score, resolve_multi_hypothesis_topology
        policy_hypotheses = hypotheses_for(state.classification)
        scored_list = []

        for item in policy_hypotheses:
            sh = evaluate_hypothesis_score(item, evidence_by_fact, state.classification)
            scored_list.append(sh)

        scored_list, landscape = resolve_multi_hypothesis_topology(scored_list)
        state.hypothesis_landscape = landscape
        return [sh.hypothesis for sh in scored_list]

    def _diagnose(self, state):
        if state.evidence_evaluation.get('diagnosis_status') != 'SUFFICIENT':
            return {
                'status': 'INSUFFICIENT_EVIDENCE',
                'confidence': 0.0,
                'code': state.classification,
                'mechanism': 'UNKNOWN',
                'root_cause': 'UNKNOWN',
                'missing_required_evidence': state.evidence_evaluation.get('required_missing', []),
                'missing_diagnostic_evidence': state.evidence_evaluation.get('missing_evidence', []),
                'statement': 'Do not claim root cause until the adaptive diagnostic evidence requirements are verified.',
                'reasoning_summary': 'The initial evidence establishes the failure signature but targeted evidence is still required to resolve the active hypothesis.',
            }
        best = max((h for h in state.hypotheses if h.status == 'SUPPORTED'), key=lambda h: h.score, default=None)
        if not best:
            if hasattr(self, 'knowledge_store') and self.knowledge_store and state.failure_signature:
                sig = self.knowledge_store.get_signature(state.failure_signature)
                if sig:
                    facts = {e.fact for e in state.evidence if e.status.value == 'SUCCESS'}
                    ev_status = self.knowledge_store.evaluate_signature_evidence(sig, facts)
                    if ev_status.value in ('CONFIRMED', 'POSSIBLE'):
                        return {
                            'status': 'LIKELY',
                            'confidence': sig.confidence_base,
                            'code': state.classification,
                            'mechanism': sig.mechanism,
                            'root_cause': 'UNKNOWN',
                            'root_cause_candidate': sig.mechanism,
                            'symptom': sig.description,
                            'statement': f"Evidence is consistent with operational pattern {sig.signature_id} ({sig.description}).",
                            'reasoning_summary': f"Dynamic failure signature {sig.signature_id} matched log patterns and live evidence evaluated to {ev_status.value}.",
                            'signature_id': sig.signature_id,
                            'dynamic_knowledge_match': True,
                        }
            return {
                'status': 'INSUFFICIENT_EVIDENCE',
                'confidence': 0.0,
                'code': state.classification,
                'mechanism': 'UNKNOWN',
                'root_cause': 'UNKNOWN',
                'statement': 'No supported hypothesis remains after evidence evaluation.',
                'reasoning_summary': 'Evidence does not support a deterministic diagnosis.',
            }

        facts = {e.fact for e in state.evidence if e.status.value == 'SUCCESS'}
        evidence_ids_by_fact: dict[str, list[str]] = {}
        for e in state.evidence:
            if e.status.value == 'SUCCESS':
                evidence_ids_by_fact.setdefault(e.fact, []).append(e.id)

        from ..rules.causal_chains import build_causal_chain
        causal = build_causal_chain(state.classification, best.code, facts, evidence_ids_by_fact)
        state.causal_chain = causal.to_dict()

        secondary_supported = [h.code for h in state.hypotheses if h.status == 'SUPPORTED' and h.id != best.id]
        landscape = getattr(state, 'hypothesis_landscape', {})

        diag_base = {
            'status': 'LIKELY',
            'confidence': best.score,
            'code': state.classification,
            'mechanism': best.code,
            'root_cause': 'UNKNOWN',
            'root_cause_candidate': causal.root_cause,
            'symptom': causal.symptom,
            'causal_chain': causal.to_dict(),
            'contributing_factors': causal.contributing_factors,
            'hypothesis_id': best.id,
            'secondary_mechanisms': secondary_supported,
            'hypothesis_topology': landscape.get('topology_status', 'EXCLUSIVE'),
        }

        if state.classification == 'STORAGE.CSI.PROVISIONING_TIMEOUT':
            if 'BACKEND_UNHEALTHY' in facts:
                return {
                    **diag_base,
                    'statement': 'CSI provisioning timed out and storage-backend evidence indicates backend degradation is contributing to the failure.',
                    'reasoning_summary': 'PVC pending, CSI timeout, and backend-unhealthy evidence support the storage backend hypothesis.',
                }
            if 'CSI_CONTROLLER_PROVISIONING_ERROR' in facts or 'PVC_PROVISIONING_FAILED' in facts:
                return {
                    **diag_base,
                    'statement': 'CSI provisioning timed out and controller/PVC provisioning evidence identifies the CSI provisioning path as the active failure mechanism while the storage backend is healthy.',
                    'reasoning_summary': 'Initial storage evidence plus targeted CSI controller/PVC event evidence support the CSI provisioning-path hypothesis.',
                }
            return {
                **diag_base,
                'statement': 'CSI provisioning timed out while the storage backend is reported healthy; the CSI/controller provisioning path remains the leading mechanism.',
                'reasoning_summary': 'Required storage evidence supports the CSI/controller provisioning-path hypothesis.',
            }
        return {
            **diag_base,
            'statement': 'Evidence supports the classified failure signature.',
            'reasoning_summary': 'Required and adaptive evidence support the strongest hypothesis.',
        }

    def _llm_advisory(self, state):
        # LLM is advisory only. It is consulted only after deterministic evidence
        # evaluation cannot establish a supported mechanism. Its output is never
        # converted into Evidence, Hypothesis, diagnosis, approval, or execution.
        if state.diagnosis.get('status') != 'INSUFFICIENT_EVIDENCE':
            return {'status': 'NOT_REQUESTED', 'reason': 'Deterministic evidence was sufficient or a supported mechanism was established.'}
        if self.llm_provider is None:
            return {'status': 'NOT_CONFIGURED', 'reason': 'No LLM provider configured. Set LLM_PROVIDER=openrouter to enable advisory suggestions.'}
        try:
            import time
            started=time.monotonic()
            response=self.llm_provider.generate(
                build_messages(state),
                temperature=0,
                max_tokens=1000,
                response_format={'type':'json_object'},
            )
            advisory=parse_advisory(response, registry=self.registry)
            advisory['latency_ms']=round((time.monotonic()-started)*1000, 1)
            advisory['provider']=type(self.llm_provider).__name__
            advisory['model']=getattr(self.llm_provider, 'model', None)
            state.trace.append(f"llm advisory status={advisory.get('status')}")
            return advisory
        except Exception as exc:
            state.trace.append('llm advisory failed')
            return {
                'status':'UNAVAILABLE',
                'reason':str(exc),
                'provider':type(self.llm_provider).__name__,
                'model':getattr(self.llm_provider, 'model', None),
            }

    def _execute_exploratory_investigation(self, state, tool, max_requests=2):
        """Executes safe, contract-validated read-only investigations suggested by LLM advisory.

        Strictly bounded:
        - Only validated read-only capabilities in the registry are invoked.
        - Max 2 requests executed per incident.
        - Zero mutations permitted.
        """
        if not hasattr(state, 'llm_advisory') or not isinstance(state.llm_advisory, dict):
            return
        if state.llm_advisory.get('status') != 'ADVISORY':
            return
        suggestions = state.llm_advisory.get('suggested_investigations', [])
        executed = []
        new_evidence_collected = []
        for item in suggestions:
            if len(executed) >= max_requests:
                break
            if not item.get('validated', False):
                continue
            cap = item.get('capability')
            params = item.get('parameters', {})
            key = self._req_key(item)
            if any(self._req_key(r) == key and r.get('status') in {'SUCCESS', 'NO_DATA'} for r in state.evidence_plan):
                continue
            req = {
                'capability': cap,
                'parameters': params,
                'purpose': item.get('purpose', 'LLM exploratory read-only investigation'),
            }
            ev_before_count = len(state.evidence)
            self._collect_round(state, tool, requirements=[req])
            executed.append(item)
            ev_after = state.evidence[ev_before_count:]
            for ev in ev_after:
                if ev.status.value == 'SUCCESS':
                    new_evidence_collected.append({
                        'id': ev.id,
                        'fact': ev.fact,
                        'source': ev.source,
                        'domain': ev.domain,
                        'signal': ev.signal,
                        'status': ev.status.value if hasattr(ev.status, 'value') else str(ev.status),
                    })
            state.trace.append(f"exploratory investigation executed capability={cap} signal={params.get('signal')} found={len(ev_after)}")

        state.llm_advisory['executed_investigations'] = executed
        state.llm_advisory['evidence_gathered'] = new_evidence_collected
        if new_evidence_collected:
            state.llm_advisory['corroboration_status'] = 'CORROBORATED'
            state.trace.append(f"exploratory evidence corroborated count={len(new_evidence_collected)}")
        elif executed:
            state.llm_advisory['corroboration_status'] = 'NO_NEW_DATA'
        else:
            state.llm_advisory['corroboration_status'] = 'NOT_EXECUTED'

    def _next_step(self,state):
        if state.status=='INSUFFICIENT_EVIDENCE':
            # A classified failure with an adaptive investigation policy has
            # a known evidence family, even after the current collection round
            # has consumed/cleared its request list. Keep directing the SRE
            # toward targeted evidence until the policy is exhausted.
            if state.next_evidence_requests:
                return 'COLLECT_TARGETED_EVIDENCE'
            adaptive = adaptive_for(state.classification)
            if state.classification != 'UNKNOWN' and adaptive.get('enabled', False) and state.evidence_round > 0:
                return 'COLLECT_TARGETED_EVIDENCE'
            return 'COLLECT_MISSING_EVIDENCE'
        facts={e.fact for e in state.evidence if e.status.value=='SUCCESS'}
        if state.classification=='STORAGE.CSI.PROVISIONING_TIMEOUT':
            if 'BACKEND_UNHEALTHY' in facts: return 'INVESTIGATE_STORAGE_BACKEND'
            if 'BACKEND_HEALTHY' in facts and ('CSI_CONTROLLER_PROVISIONING_ERROR' in facts or 'PVC_PROVISIONING_FAILED' in facts): return 'REVIEW_CSI_CONTROLLER_FAILURE'
            if 'BACKEND_HEALTHY' in facts: return 'INVESTIGATE_CSI_CONTROLLER'
        if state.classification=='NETWORK.NAD.MISSING': return 'VERIFY_NAD_CONFIGURATION'
        if state.classification=='VMWARE.CBT': return 'INVESTIGATE_VMWARE_CBT'
        if state.classification=='VMWARE.ESXI.CONNECTIVITY': return 'INVESTIGATE_ESXI_CONNECTIVITY'
        if state.classification in {'GUEST.WINDOWS.VSS', 'VMWARE.GUEST.VSS'}: return 'INVESTIGATE_WINDOWS_VSS'
        if state.classification == 'GUEST.WINDOWS.REGISTRY': return 'REPAIR_GUEST_REGISTRY'
        if state.classification == 'GUEST.WINDOWS.BITLOCKER': return 'DISABLE_OR_UNLOCK_BITLOCKER'
        if state.classification == 'OCV.VM.FIRMWARE': return 'RECONCILE_VM_BOOTLOADER'
        if state.classification in {'GUEST.LINUX.BTRFS', 'GUEST.LINUX.BTRFS_UNSUPPORTED'}: return 'CONVERT_OR_EXCLUDE_BTRFS'
        if state.classification == 'VMWARE.VDDK.PERMISSION': return 'CHECK_VDDK_PERMISSIONS'
        if state.classification == 'VMWARE.VDDK.DATA_SOURCE': return 'VERIFY_VDDK_NBD_EXPORT'
        if state.classification == 'CONVERSION.VIRT_V2V.CDROM': return 'DETACH_CDROM_ISO'
        if state.classification == 'CONVERSION.VIRT_V2V.OOM': return 'INCREASE_CONVERSION_POD_MEMORY'
        if state.classification == 'CONVERSION.IMAGE_CONVERSION.ARG_LIST': return 'REDUCE_ATTACHED_DISKS'
        if state.classification == 'OS.WINDOWS.FILESYSTEM_READONLY': return 'SHUTDOWN_GUEST_CLEANLY'
        if state.classification == 'DISK.RESIZE_FAILED': return 'EXPAND_TARGET_STORAGE'
        if state.classification in {'OS.WINDOWS.VIRTIO_DRIVERS_MISSING', 'GUEST.WINDOWS.VIRTIO', 'GUEST.WINDOWS.VIRTIO_DRIVER'}: return 'INJECT_VIRTIO_DRIVERS'
        if state.classification == 'VMWARE.VMDK.NOT_FOUND': return 'VERIFY_DATASTORE_VMDK'
        if state.classification == 'VMWARE.CREDENTIALS.UNAUTHORIZED': return 'UPDATE_PROVIDER_CREDENTIALS'
        if hasattr(self, 'knowledge_store') and self.knowledge_store and state.failure_signature:
            sig = self.knowledge_store.get_signature(state.failure_signature)
            if sig:
                sol = self.knowledge_store.get_best_solution(sig, facts, state.context)
                if sol and sol.action_plan and sol.action_plan.steps:
                    return sol.action_plan.steps[0].action_type.value
        for r in state.recovery:
            if r.action=='RETRY' and r.readiness==Readiness.READY: return 'RETRY'
        return 'SRE_REVIEW'

    def _diagnosis_basis(self, state):
        success = [e for e in state.evidence if e.status.value == 'SUCCESS']
        evidence_ids_by_fact: dict[str, list[str]] = {}
        for e in success:
            evidence_ids_by_fact.setdefault(e.fact, []).append(e.id)
        evidence_by_fact = {k: v[0] for k, v in evidence_ids_by_fact.items()}
        supporting = []
        contradicting = []
        excluded = []

        for h in state.hypotheses:
            item = {
                'hypothesis_id': h.id,
                'mechanism': h.code,
                'status': h.status,
                'evidence_ids': list(h.supporting),
            }
            if h.status == 'SUPPORTED':
                supporting.append(item)
            if h.contradicting:
                contradicting.append({
                    'hypothesis_id': h.id,
                    'mechanism': h.code,
                    'evidence_ids': list(h.contradicting),
                })
            if h.status == 'CONTRADICTED':
                contra_facts = [e.fact for e in success if e.id in h.contradicting]
                contra_str = f"reports {', '.join(contra_facts)}" if contra_facts else "contradicts this mechanism"
                if not any(x['mechanism'] == h.code for x in excluded):
                    excluded.append({
                        'mechanism': h.code,
                        'reason': f"Current evidence {contra_str}.",
                        'evidence_ids': list(h.contradicting),
                    })

        current = state.diagnosis.get('mechanism')
        if state.classification == 'STORAGE.CSI.PROVISIONING_TIMEOUT' and 'BACKEND_HEALTHY' in evidence_by_fact:
            if current != 'STORAGE.BACKEND_DEGRADED' and not any(x['mechanism'] == 'STORAGE.BACKEND_DEGRADED' for x in excluded):
                excluded.append({
                    'mechanism': 'STORAGE.BACKEND_DEGRADED',
                    'reason': 'Current storage backend evidence reports BACKEND_HEALTHY.',
                    'evidence_ids': [evidence_by_fact['BACKEND_HEALTHY']],
                })
        if state.classification == 'STORAGE.CSI.PROVISIONING_TIMEOUT' and 'BACKEND_UNHEALTHY' in evidence_by_fact:
            if current != 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH':
                excluded.append({
                    'mechanism': 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH',
                    'reason': 'Current evidence reports BACKEND_UNHEALTHY and no controller-path failure is established.',
                    'evidence_ids': [evidence_by_fact['BACKEND_UNHEALTHY']],
                })

        memory_context = []
        for x in state.historical_context[:5]:
            memory_context.append({
                'source': 'SRE_TRACKER',
                'id': x.get('failure_case_id') or x.get('event_id'),
                'role': 'CONTEXT',
                'used_for': 'Historical correlation only',
            })
        for x in state.knowledge_context[:5]:
            memory_context.append({
                'source': 'RHOKP',
                'id': x.get('id'),
                'role': 'CONTEXT',
                'used_for': 'Documented troubleshooting context only',
            })

        return {
            'supporting_evidence': supporting,
            'contradicting_evidence': contradicting,
            'excluded_mechanisms': excluded,
            'memory_context': memory_context,
            'priority_order': ['CURRENT_EVIDENCE', 'SRE_HISTORY', 'RHOKP_KNOWLEDGE', 'LLM_ADVISORY'],
            # Backward-compatible aliases. These are identifiers only, not authority claims.
            'authoritative': [e.id for e in success],
            'historical': [str(x.get('failure_case_id', x.get('event_id', ''))) for x in state.historical_context if x.get('failure_case_id') or x.get('event_id')],
            'knowledge': [str(x.get('id', '')) for x in state.knowledge_context if x.get('id')],
            'advisory': [],
        }

    def _memory_enrichment(self, state):
        enrich = []
        if state.historical_context:
            enrich.append({
                'source': 'SRE_TRACKER',
                'count': len(state.historical_context),
                'records': [{
                    'failure_case_id': x.get('failure_case_id'),
                    'status': x.get('status'),
                    'resolution_code': x.get('resolution_code'),
                } for x in state.historical_context[:5]],
            })
        if state.periodic_context:
            enrich.append({
                'source': 'SRE_PERIODIC',
                'count': len(state.periodic_context),
                'patterns': [{
                    'pattern_id': x.get('pattern_id'),
                    'occurrence_count': x.get('occurrence_count'),
                    'trend': x.get('trend'),
                } for x in state.periodic_context[:5]],
            })
        if state.knowledge_context:
            enrich.append({
                'source': 'RHOKP',
                'count': len(state.knowledge_context),
                'records': [{
                    'id': x.get('id'),
                    'title': x.get('title'),
                    'kind': x.get('kind'),
                    'url': x.get('url'),
                } for x in state.knowledge_context[:5]],
            })

        current_mechanism = state.diagnosis.get('mechanism')
        conflicts = []
        for x in state.historical_context:
            resolution = str(x.get('resolution_code', '')).upper()
            if current_mechanism == 'STORAGE.BACKEND_DEGRADED' and 'CSI_CONTROLLER' in resolution:
                conflicts.append(x.get('failure_case_id'))
            elif current_mechanism == 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH' and ('BACKEND' in resolution or 'STORAGE_BACKEND' in resolution):
                conflicts.append(x.get('failure_case_id'))
        if conflicts:
            enrich.append({
                'source': 'MEMORY_CONFLICT',
                'records': conflicts,
                'note': 'Historical context conflicts with current evidence; current evidence remains authoritative.',
            })
        return enrich

    def _recommendation(self, state):
        package = self._investigation_package(state)
        insufficient = state.diagnosis.get('status') == 'INSUFFICIENT_EVIDENCE'
        next_action = (
            package['next_investigations'][0]['action'] if insufficient and package['next_investigations']
            else (state.next_step or 'SRE_REVIEW')
        )
        investigate_next = (
            [x['action'] for x in package['next_investigations']] if insufficient
            else [state.next_step] if state.next_step else []
        )
        rec = {
            'type': 'INVESTIGATION',
            'code': next_action,
            'priority': package['priority'],
            'finding': package['finding'],
            'why': state.diagnosis.get('reasoning_summary', ''),
            'investigate_next': investigate_next,
            'memory_enrichment': self._memory_enrichment(state),
            'blocked_actions': package['do_not_do'],
            'do_not_do': package['do_not_do'],
            'approval_required': False,
            'success_conditions': package['success_conditions'],
            'recurrence': state.recurrence,
            'learning': state.learning,
        }

        # Dynamic Knowledge Store integration
        if hasattr(self, 'knowledge_store') and self.knowledge_store:
            facts = {e.fact for e in state.evidence if e.status.value == 'SUCCESS'}
            sig = None
            if state.failure_signature:
                sig = self.knowledge_store.get_signature(state.failure_signature)
            if not sig:
                msg = state.event.get('message', '') or state.event.get('error', '')
                matches = self.knowledge_store.match_failure(msg, state.context)
                if matches:
                    sig = matches[0][0]
                    state.failure_signature = sig.signature_id

            if sig:
                ev_status = self.knowledge_store.evaluate_signature_evidence(sig, facts)
                rec['signature_id'] = sig.signature_id
                rec['signature_domain'] = sig.domain
                rec['signature_evidence_status'] = ev_status.value
                if ev_status.value != 'REJECTED':
                    best_sol = self.knowledge_store.get_best_solution(sig, facts, state.context)
                    if best_sol:
                        rec['action_plan'] = best_sol.action_plan.to_dict()
                        rec['recommended_solution'] = best_sol.to_dict()
                        rec['recommended_action'] = best_sol.recommended_action
                        rec['action_summary'] = best_sol.action_summary
                        rec['requires_approval'] = best_sol.requires_approval
                        rec['approval_role'] = best_sol.approval_role
                        if not insufficient:
                            rec['finding'] = best_sol.action_summary
                            rec['why'] = f"{best_sol.title}. {best_sol.action_plan.rationale}"
                            if best_sol.action_plan.steps:
                                rec['code'] = best_sol.action_plan.steps[0].action_type.value

        # Ingest unknown or unconfirmed failures into learning pipeline
        if hasattr(self, 'learning_pipeline') and self.learning_pipeline:
            if state.classification == 'UNKNOWN' or state.diagnosis.get('status') == 'INSUFFICIENT_EVIDENCE':
                raw_log = state.event.get('message', '') or state.event.get('error', '') or state.event.get('description', '')
                if raw_log:
                    adv = state.llm_advisory if hasattr(state, 'llm_advisory') and isinstance(state.llm_advisory, dict) else {}
                    adv_hyp = adv.get('parametric_hypothesis')
                    sre_diag = adv.get('suggested_sre_diagnostics', [])
                    cand = self.learning_pipeline.ingest_unknown_failure(
                        raw_log=raw_log,
                        suggested_mechanism=adv_hyp or state.diagnosis.get('mechanism', 'UNKNOWN'),
                        suggested_action=state.next_step or 'SRE_REVIEW',
                        metadata={
                            'failure_case_id': state.failure_case_id,
                            'cluster_id': state.event.get('cluster_id'),
                            'parametric_hypothesis': adv_hyp,
                            'sre_diagnostics': sre_diag,
                        },
                    )
                    rec['learning_candidate_id'] = cand.candidate_id
                    if adv_hyp:
                        rec['parametric_hypothesis'] = adv_hyp
                    if sre_diag:
                        rec['suggested_sre_diagnostics'] = sre_diag

        return rec

    def _investigation_package(self, state):
        facts = {e.fact for e in state.evidence if e.status.value == 'SUCCESS'}
        investigations = []
        seen = set()
        # Preserve the actual adaptive investigation requests that led to the diagnosis.
        for history in state.investigation_history:
            for req in history.get('requests', []):
                p = req.get('parameters', {})
                key = self._req_key(req)
                if key in seen:
                    continue
                seen.add(key)
                investigations.append({
                    'action': self._action_name(p.get('signal'), req.get('capability')),
                    'purpose': req.get('purpose', 'Collect targeted evidence.'),
                    'required_capability': req.get('capability'),
                    'parameters': p,
                    'round': history.get('round', state.evidence_round),
                })
        for req in state.next_evidence_requests:
            p = req.get('parameters', {})
            key = self._req_key(req)
            if key in seen:
                continue
            seen.add(key)
            investigations.append({
                'action': self._action_name(p.get('signal'), req.get('capability')),
                'purpose': req.get('purpose', 'Collect targeted evidence.'),
                'required_capability': req.get('capability'),
                'parameters': p,
                'round': state.evidence_round,
            })

        if not investigations and state.diagnosis.get('status') == 'LIKELY':
            policy = load_policy(state.classification)
            for req in policy.get('required_evidence', []):
                p = req.get('parameters', {})
                key = self._req_key(req)
                if key in seen:
                    continue
                seen.add(key)
                investigations.append({
                    'action': self._action_name(p.get('signal'), req.get('capability')),
                    'purpose': req.get('purpose', 'Collect targeted evidence according to policy.'),
                    'required_capability': req.get('capability'),
                    'parameters': p,
                    'round': state.evidence_round,
                })

        insufficient = state.diagnosis.get('status') == 'INSUFFICIENT_EVIDENCE'
        if insufficient and not investigations:
            # Required evidence can be unavailable/NO_DATA/ERROR before any adaptive
            # round exists. Preserve those concrete requests so the SRE result says
            # exactly what needs to be collected instead of only saying "collect missing evidence".
            required_missing = state.evidence_evaluation.get('required_missing', [])
            if required_missing:
                for item in required_missing:
                    if isinstance(item, str) and ':' in item:
                        capability, rest = item.split(':', 1)
                        domain, signal = rest.split('/', 1) if '/' in rest else ('', rest)
                        investigations.append({
                            'action': self._action_name(signal, capability),
                            'purpose': 'Collect required evidence that was unavailable or returned no data.',
                            'required_capability': capability,
                            'parameters': {'domain': domain, 'signal': signal},
                            'round': state.evidence_round + 1,
                        })
            for req in state.evidence_evaluation.get('missing_evidence', []):
                p = req.get('parameters', {})
                investigations.append({
                    'action': self._action_name(p.get('signal'), req.get('capability')),
                    'purpose': req.get('purpose', 'Collect missing diagnostic evidence.'),
                    'required_capability': req.get('capability'),
                    'parameters': p,
                    'round': state.evidence_round + 1,
                })

        if insufficient:
            finding = state.diagnosis.get('statement', 'Diagnostic evidence is insufficient to establish a mechanism.')
            do_not_do = ['Do not claim root cause.', 'Do not execute remediation or retry from this agent.']
            success = ['Collect the missing diagnostic evidence.', 'Re-evaluate the mechanism using current evidence.']
        else:
            finding = state.diagnosis.get('statement', '')
            do_not_do = ['Do not execute remediation or retry from this agent.']
            if state.diagnosis.get('mechanism') == 'UNKNOWN':
                do_not_do.insert(0, 'Do not claim a specific root cause.')
            success = ['Investigation mechanism is understood and evidence-backed.', 'Retry/remediation readiness is evaluated separately from diagnosis.']

        package={
            'priority': 'HIGH' if state.diagnosis.get('status') != 'INSUFFICIENT_EVIDENCE' else 'HIGH',
            'finding': finding,
            'next_investigations': investigations,
            'do_not_do': do_not_do,
            'success_conditions': success,
        }
        if state.llm_advisory.get('status') == 'ADVISORY':
            adv = state.llm_advisory
            package['llm_advisory'] = {
                'summary': adv.get('summary', ''),
                'parametric_hypothesis': adv.get('parametric_hypothesis', ''),
                'suggested_investigations': adv.get('suggested_investigations', []),
                'suggested_sre_diagnostics': adv.get('suggested_sre_diagnostics', []),
                'executed_investigations': adv.get('executed_investigations', []),
                'evidence_gathered': adv.get('evidence_gathered', []),
                'corroboration_status': adv.get('corroboration_status', 'NOT_EXECUTED'),
                'uncertainty': adv.get('uncertainty', ''),
                'warning': (
                    'Read-only exploratory investigation was executed; review corroboration status.'
                    if adv.get('executed_investigations')
                    else 'Advisory only. Suggestions were not executed and are not evidence.'
                ),
            }
            if adv.get('suggested_sre_diagnostics'):
                package['suggested_sre_diagnostics'] = adv.get('suggested_sre_diagnostics', [])
            if adv.get('parametric_hypothesis'):
                package['parametric_hypothesis'] = adv.get('parametric_hypothesis', '')
        package['environment'] = state.environment.to_dict() if state.environment else {}
        package['recurrence'] = state.recurrence
        package['learning'] = state.learning
        package['knowledge_gap'] = state.learning.get('knowledge_gap', False)
        package['knowledge_compatibility'] = state.compatibility_context
        package['version_conflicts'] = [
            {'id': x.get('id', x.get('document_id')), 'title': x.get('title'), 'applicability': x.get('applicability')}
            for x in state.version_conflicts
        ]
        return package

    @staticmethod
    def _action_name(signal, capability):
        if not signal:
            return 'COLLECT_TARGETED_EVIDENCE'
        return 'INVESTIGATE_' + str(signal).upper().replace('-', '_').replace('/', '_')

    def _decision_readiness(self, state):
        by_action = {r.action: r for r in state.recovery}
        def item(action, evaluation):
            r = by_action[action]
            return {
                'status': r.readiness.value,
                'blockers': list(r.blockers),
                'evaluation': evaluation,
                'execution': 'NOT_PERFORMED',
                'approval_required': r.approval_required,
            }
        return {
            'CONTINUE_MONITOR': item('CONTINUE_MONITOR', 'Whether current migration state supports continued observation.'),
            'RETRY': item('RETRY', 'Whether classification-specific deterministic retry preconditions are verified.'),
            'FIX_FORWARD': item('FIX_FORWARD', 'Whether a deterministic fix-forward procedure has been evaluated. No remediation is executed here.'),
            'ROLLBACK': item('ROLLBACK', 'Whether rollback capability and procedure have been evaluated.'),
            'ESCALATE': item('ESCALATE', 'Whether escalation is an appropriate current decision path.'),
        }

def run_agent(request, registry=None, **kwargs):
    if registry is None: raise RuntimeError('Capability Registry is required; production must inject it.')
    return MigrationFailureEngine(registry,**kwargs).run(request)
def run_to_dict(state): return state.to_dict()
