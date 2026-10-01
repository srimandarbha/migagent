from pathlib import Path
from ..contracts import AgentState, Hypothesis, RecoveryOption, Readiness
from ..rules.classification import classify
from ..rules.evidence_policy import required_for, optional_for, adaptive_for, load_policy
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
from ..rules.learning import FIRST_SEEN, RECURRING_UNKNOWN, RECURRING_UNRESOLVED, RECURRING_RESOLVED, KNOWN_ISSUE

class MigrationFailureEngine:
    def __init__(self, registry, max_iterations=12, tracker=None, knowledge=None, skill_root=None, memory_mode='both', llm_provider=None):
        self.registry=registry; self.max_iterations=max_iterations; self.tracker=tracker; self.knowledge=knowledge; self.memory_mode=memory_mode
        self.llm_provider = llm_provider if llm_provider is not None else build_llm_provider()
        self.skill_repo=SkillRepository(skill_root or Path(__file__).parents[2] / 'skills')
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
        event=request.get('event', request)
        failure_case_id=request.get('failure_case_id', event.get('failure_case_id'))
        if not failure_case_id:
            failure_case_id=event.get('event_id') or request.get('incident_id')
        if not failure_case_id: raise ValueError('Migration Failure Agent requires failure_case_id')
        state=AgentState(failure_case_id=str(failure_case_id), event=event, success_conditions=['failure confirmed','failed phase identified','required evidence verified','diagnosis evidence-backed','next step safety-gated'])
        state.context={k:v for k,v in event.items() if k not in ('hidden_truth','truth','answer_key')}
        state.trace.append('context loaded')
        state.classification,state.classification_confidence=classify(event); state.trace.append(f'classify {state.classification}')
        policy=load_policy(state.classification); state.policy_version=policy.get('version')
        if self.tracker:
            try:
                case_id=self.tracker.create_or_get_failure_case(event_id=str(event.get('event_id', state.failure_case_id)), migration_id=str(event.get('migration_id','unknown')), vm_id=event.get('vm_id'), cluster_id=event.get('cluster_id'), change_id=event.get('change_id'), failure_class=state.classification, failure_code=state.classification.split('.')[-1], agent_version=state.agent_version, policy_version=state.policy_version, severity=event.get('severity'))
                state.failure_case_id=str(case_id)
                kafka=request.get('kafka', {})
                if kafka and hasattr(self.tracker, 'save_event'):
                    from datetime import datetime, timezone
                    self.tracker.save_event(event_id=str(event.get('event_id', state.failure_case_id)), failure_case_id=case_id, event_type=str(event.get('event_type','MigrationFailed')), event_time=datetime.fromisoformat(str(kafka.get('event_time')).replace('Z','+00:00')) if kafka.get('event_time') else datetime.now(timezone.utc), payload=event, kafka_topic=kafka.get('topic'), kafka_partition=kafka.get('partition'), kafka_offset=kafka.get('offset'))
            except Exception as exc: state.errors.append(f'tracker case/event persistence: {exc}')
        state.skill_id=policy.get('skill')
        if state.skill_id and self.skill_repo.exists(state.skill_id):
            state.skill=self.skill_repo.load(state.skill_id); state.trace.append(f'skill loaded {state.skill_id}')
        else: state.errors.append(f'skill unavailable: {state.skill_id}')

        memory_mode=request.get('memory_mode', self.memory_mode)
        self._load_environment(state)
        MemoryContext(self.tracker,self.knowledge).load(state, memory_mode)
        self._check_knowledge_compatibility(state)
        state.knowledge_judge = self._judge_knowledge_applicability(state)
        self._correlate_recurrence(state)
        tool=InvestigationTool(self.registry,self.tracker,self.knowledge)
        self._initialize_evidence_plan(state)
        self._collect_round(state, tool, required_only=True)

        # Adaptive loop: the evaluator decides whether the current evidence answers the
        # current uncertainty. Only policy-defined capabilities can be selected.
        max_rounds=int(adaptive_for(state.classification).get('max_rounds', 0) or 0)
        while True:
            evaluation=self._evaluate_evidence(state)
            state.evidence_evaluation=evaluation
            state.trace.append(f'evidence evaluation={evaluation["status"]}')
            if evaluation['diagnosis_status']=='SUFFICIENT' or evaluation['status']=='BLOCKED':
                break
            if not adaptive_for(state.classification).get('enabled', False) or state.evidence_round >= max_rounds:
                state.trace.append('adaptive investigation stopped: round limit or disabled')
                break
            requests=self._select_next_evidence(state)
            state.next_evidence_requests=requests
            if not requests:
                state.trace.append('adaptive investigation stopped: no approved next evidence')
                break
            state.evidence_round += 1
            state.investigation_history.append({'round':state.evidence_round,'requests':requests})
            state.trace.append(f'adaptive evidence round={state.evidence_round} requests={len(requests)}')
            self._collect_round(state, tool, requirements=requests)
            if state.iteration >= self.max_iterations:
                state.trace.append('iteration limit reached')
                break

        # Optional telemetry is collected only after the diagnostic evidence gate has
        # been evaluated. It cannot turn an insufficient diagnosis into a sufficient one.
        if state.evidence_evaluation.get('diagnosis_status')=='SUFFICIENT':
            for req in optional_for(state.classification)[:1]:
                self._collect_round(state, tool, requirements=[req])

        state.hypotheses=self._hypotheses(state)
        state.trace.append(f'hypotheses evaluated={len(state.hypotheses)}')
        state.diagnosis=self._diagnose(state)
        state.llm_advisory=self._llm_advisory(state)
        state.mechanism=state.diagnosis.get('mechanism')
        state.diagnosis_basis=self._diagnosis_basis(state)
        state.evidence_evaluation['diagnosis_status']='SUFFICIENT' if state.diagnosis.get('status')!='INSUFFICIENT_EVIDENCE' else 'INSUFFICIENT'
        # v2.8.3: decision readiness is a structured diagnostic contract.
        # Legacy recovery is retained only so existing callers/tests do not break.
        state.recovery.clear()
        for action in ['CONTINUE_MONITOR','RETRY','FIX_FORWARD','ROLLBACK','ESCALATE']:
            readiness, blockers=gate_recovery(state,action)
            state.recovery.append(RecoveryOption(action,readiness,blockers,approval_required=action not in ('CONTINUE_MONITOR',)))
        state.status='COMPLETED' if state.diagnosis['status']!='INSUFFICIENT_EVIDENCE' else 'INSUFFICIENT_EVIDENCE'
        state.next_step=self._next_step(state)
        state.recommendation=self._recommendation(state)
        state.investigation_package=self._investigation_package(state)
        state.decision_readiness=self._decision_readiness(state)
        self._persist_results(state)
        return state

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
        state.classification,state.classification_confidence=classify(state.event)
        state.trace.append(f'classify {state.classification}')
        policy=load_policy(state.classification)
        state.policy_version=policy.get('version')
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
        state.failure_signature = build_base_signature(state.event, state.classification)
        result = correlate_recurrence(
            self.tracker,
            signature=state.failure_signature,
            failure_case_id=state.failure_case_id,
            classification=state.classification,
            cluster_id=state.event.get('cluster_id'),
        )
        state.recurrence = result
        state.recurrence.setdefault('signature', state.failure_signature)
        state.learning = {
            'status': 'NOT_EVALUATED',
            'resolution_recorded': bool(result.get('previous_outcomes')),
            'validated_solution_exists': bool(result.get('validated_solution_refs')),
            'knowledge_gap': result.get('recurrence_status') in {FIRST_SEEN, RECURRING_UNKNOWN, RECURRING_UNRESOLVED},
        }
        if result.get('status') == 'DISABLED':
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
        return LearningLifecycle(self.tracker).record_resolution(
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
        failed=[r for r in state.capability_results if r.get('status') in {'NO_DATA','UNKNOWN','UNAVAILABLE','ERROR'}]
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
        facts = {e.fact for e in state.evidence if e.status.value == 'SUCCESS'}
        refs = {e.fact: e.id for e in state.evidence if e.status.value == 'SUCCESS'}
        hypotheses = []
        if state.classification == 'STORAGE.CSI.PROVISIONING_TIMEOUT':
            if 'BACKEND_UNHEALTHY' in facts:
                hypotheses.append(Hypothesis(id='H-STORAGE-BACKEND', code='STORAGE.BACKEND_DEGRADED', description='Storage backend degradation is contributing to CSI provisioning timeout.', score=0.90, status='SUPPORTED', supporting=[x for x in (refs.get('BACKEND_UNHEALTHY'),refs.get('PVC_PENDING'),refs.get('CSI_PROVISIONING_TIMEOUT')) if x], contradicting=[]))
            if 'BACKEND_HEALTHY' in facts:
                if 'CSI_CONTROLLER_PROVISIONING_ERROR' in facts or 'PVC_PROVISIONING_FAILED' in facts:
                    hypotheses.append(Hypothesis(id='H-CSI-PATH', code='STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH', description='CSI controller/provisioning failure is evidenced while the storage backend is healthy.', score=0.92, status='SUPPORTED', supporting=[x for x in (refs.get('BACKEND_HEALTHY'),refs.get('PVC_PENDING'),refs.get('CSI_PROVISIONING_TIMEOUT'),refs.get('CSI_CONTROLLER_PROVISIONING_ERROR'),refs.get('PVC_PROVISIONING_FAILED')) if x], contradicting=[]))
                else:
                    hypotheses.append(Hypothesis(id='H-CSI-PATH', code='STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH', description='The CSI/controller or provisioning path is implicated while the storage backend is healthy.', score=0.85, status='SUPPORTED', supporting=[x for x in (refs.get('BACKEND_HEALTHY'),refs.get('PVC_PENDING'),refs.get('CSI_PROVISIONING_TIMEOUT')) if x], contradicting=[]))
        elif state.classification == 'NETWORK.NAD.MISSING' and 'NAD_MISSING' in facts:
            hypotheses.append(Hypothesis(id='H-NAD', code='NETWORK.NAD_CONFIGURATION', description='Required network attachment definition is missing or unavailable.', score=0.90, status='SUPPORTED', supporting=[refs['NAD_MISSING']], contradicting=[]))
        elif state.classification == 'VMWARE.CBT' and 'CBT_FAILED' in facts:
            hypotheses.append(Hypothesis(id='H-CBT', code='VMWARE.CBT_STATE', description='VMware Changed Block Tracking state is implicated in the source transfer failure.', score=0.90, status='SUPPORTED', supporting=[refs['CBT_FAILED']], contradicting=[]))
        elif state.classification == 'VMWARE.ESXI.CONNECTIVITY' and 'ESXI_PORT443_UNREACHABLE' in facts:
            hypotheses.append(Hypothesis(id='H-ESXI-CONNECTIVITY', code='VMWARE.ESXI_CONNECTIVITY', description='Current evidence indicates the target environment cannot reach the ESXi endpoint on TCP 443.', score=0.90, status='SUPPORTED', supporting=[refs['ESXI_PORT443_UNREACHABLE']], contradicting=[]))
        return hypotheses

    def _diagnose(self,state):
        if state.evidence_evaluation.get('diagnosis_status')!='SUFFICIENT':
            return {'status':'INSUFFICIENT_EVIDENCE','confidence':0.0,'code':state.classification,'mechanism':'UNKNOWN','root_cause':'UNKNOWN','missing_required_evidence':state.evidence_evaluation.get('required_missing',[]),'missing_diagnostic_evidence':state.evidence_evaluation.get('missing_evidence',[]),'statement':'Do not claim root cause until the adaptive diagnostic evidence requirements are verified.','reasoning_summary':'The initial evidence establishes the failure signature but targeted evidence is still required to resolve the active hypothesis.'}
        best=max((h for h in state.hypotheses if h.status=='SUPPORTED'), key=lambda h:h.score, default=None)
        if not best:
            return {'status':'INSUFFICIENT_EVIDENCE','confidence':0.0,'code':state.classification,'mechanism':'UNKNOWN','root_cause':'UNKNOWN','statement':'No supported hypothesis remains after evidence evaluation.','reasoning_summary':'Evidence does not support a deterministic diagnosis.'}
        facts={e.fact for e in state.evidence if e.status.value=='SUCCESS'}
        if state.classification=='STORAGE.CSI.PROVISIONING_TIMEOUT':
            if 'BACKEND_UNHEALTHY' in facts:
                return {'status':'LIKELY','confidence':best.score,'code':state.classification,'mechanism':best.code,'root_cause':'UNKNOWN','statement':'CSI provisioning timed out and storage-backend evidence indicates backend degradation is contributing to the failure.','reasoning_summary':'PVC pending, CSI timeout, and backend-unhealthy evidence support the storage backend hypothesis.','hypothesis_id':best.id}
            if 'CSI_CONTROLLER_PROVISIONING_ERROR' in facts or 'PVC_PROVISIONING_FAILED' in facts:
                return {'status':'LIKELY','confidence':best.score,'code':state.classification,'mechanism':best.code,'root_cause':'UNKNOWN','statement':'CSI provisioning timed out and controller/PVC provisioning evidence identifies the CSI provisioning path as the active failure mechanism while the storage backend is healthy.','reasoning_summary':'Initial storage evidence plus targeted CSI controller/PVC event evidence support the CSI provisioning-path hypothesis.','hypothesis_id':best.id}
            return {'status':'LIKELY','confidence':best.score,'code':state.classification,'mechanism':best.code,'root_cause':'UNKNOWN','statement':'CSI provisioning timed out while the storage backend is reported healthy; the CSI/controller provisioning path remains the leading mechanism.','reasoning_summary':'Required storage evidence supports the CSI/controller provisioning-path hypothesis.','hypothesis_id':best.id}
        return {'status':'LIKELY','confidence':best.score,'code':state.classification,'mechanism':best.code,'root_cause':'UNKNOWN','statement':'Evidence supports the classified failure signature.','reasoning_summary':'Required and adaptive evidence support the strongest hypothesis.','hypothesis_id':best.id}

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
            advisory=parse_advisory(response)
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
        for r in state.recovery:
            if r.action=='RETRY' and r.readiness==Readiness.READY: return 'RETRY'
        return 'SRE_REVIEW'

    def _diagnosis_basis(self, state):
        success = [e for e in state.evidence if e.status.value == 'SUCCESS']
        evidence_by_fact = {e.fact: e.id for e in success}
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

        current = state.diagnosis.get('mechanism')
        if state.classification == 'STORAGE.CSI.PROVISIONING_TIMEOUT' and 'BACKEND_HEALTHY' in evidence_by_fact:
            if current != 'STORAGE.BACKEND_DEGRADED':
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
        return {
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
            if state.classification == 'STORAGE.CSI.PROVISIONING_TIMEOUT':
                if 'BACKEND_UNHEALTHY' in facts:
                    investigations = [
                        {'action': 'INSPECT_STORAGE_BACKEND_HEALTH', 'purpose': 'Determine whether backend health caused or contributed to CSI provisioning timeout.', 'required_capability': 'observability.search', 'parameters': {'domain': 'storage', 'signal': 'backend_health'}, 'round': state.evidence_round},
                        {'action': 'INSPECT_PVC_PROVISIONING_STATE', 'purpose': 'Correlate PVC state with the storage provisioning failure.', 'required_capability': 'observability.search', 'parameters': {'domain': 'ocv', 'signal': 'pvc_state'}, 'round': state.evidence_round},
                    ]
                elif 'BACKEND_HEALTHY' in facts:
                    investigations = [
                        {'action': 'INSPECT_CSI_CONTROLLER_ERRORS', 'purpose': 'Determine whether the CSI controller/provisioner reported the provisioning failure.', 'required_capability': 'observability.search', 'parameters': {'domain': 'ocv', 'signal': 'csi_controller_errors'}, 'round': state.evidence_round},
                        {'action': 'INSPECT_PVC_EVENTS', 'purpose': 'Identify Kubernetes PVC provisioning events associated with the timeout.', 'required_capability': 'observability.search', 'parameters': {'domain': 'ocv', 'signal': 'pvc_events'}, 'round': state.evidence_round},
                        {'action': 'INSPECT_STORAGECLASS_VOLUMEATTACHMENT', 'purpose': 'Check target storage configuration and attachment context before any retry decision.', 'required_capability': 'observability.search', 'parameters': {'domain': 'ocv', 'signal': 'storageclass_volumeattachment'}, 'round': state.evidence_round},
                    ]
            elif state.classification == 'NETWORK.NAD.MISSING':
                investigations = [
                    {'action': 'VERIFY_NAD_DEFINITION', 'purpose': 'Confirm the expected NetworkAttachmentDefinition exists in the target namespace.', 'required_capability': 'observability.search', 'parameters': {'domain': 'ocv', 'signal': 'nad_state'}, 'round': state.evidence_round},
                    {'action': 'INSPECT_NETWORK_ATTACHMENT_EVENTS', 'purpose': 'Correlate the migration failure with network attachment events.', 'required_capability': 'observability.search', 'parameters': {'domain': 'ocv', 'signal': 'network_events'}, 'round': state.evidence_round},
                ]
            elif state.classification == 'VMWARE.CBT':
                investigations = [
                    {'action': 'INSPECT_VMWARE_CBT_STATE', 'purpose': 'Confirm Changed Block Tracking state on the source VM.', 'required_capability': 'observability.search', 'parameters': {'domain': 'vmware', 'signal': 'cbt_state'}, 'round': state.evidence_round},
                    {'action': 'INSPECT_MTV_TRANSFER_ERRORS', 'purpose': 'Correlate CBT state with the MTV transfer failure.', 'required_capability': 'observability.search', 'parameters': {'domain': 'mtv', 'signal': 'transfer_errors'}, 'round': state.evidence_round},
                ]
            elif state.classification == 'VMWARE.ESXI.CONNECTIVITY':
                investigations = [
                    {'action': 'INSPECT_ESXI_CONNECTIVITY', 'purpose': 'Confirm network and port 443 connectivity to ESXi host.', 'required_capability': 'observability.search', 'parameters': {'domain': 'vmware', 'signal': 'esxi_connectivity'}, 'round': state.evidence_round},
                    {'action': 'INSPECT_MTV_MIGRATION_STATE', 'purpose': 'Correlate ESXi connectivity state with MTV migration state.', 'required_capability': 'observability.search', 'parameters': {'domain': 'mtv', 'signal': 'migration_state'}, 'round': state.evidence_round},
                ]

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
            package['llm_advisory'] = {
                'summary': state.llm_advisory.get('summary',''),
                'suggested_investigations': state.llm_advisory.get('suggested_investigations',[]),
                'uncertainty': state.llm_advisory.get('uncertainty',''),
                'warning': 'Advisory only. Suggestions were not executed and are not evidence.'
            }
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
