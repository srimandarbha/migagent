class MemoryContext:
    def __init__(self, tracker=None, knowledge=None):
        self.tracker=tracker; self.knowledge=knowledge

    @staticmethod
    def _failure_class(classification):
        parts=str(classification or "UNKNOWN").split(".")
        return ".".join(parts[:2]) if len(parts) >= 3 else str(classification or "UNKNOWN")

    def load(self, state, mode="both"):
        mode=str(mode or "both").lower()
        if mode not in {"both","sre","rhokp","none"}:
            raise ValueError(f"unsupported memory mode: {mode}")
        state.memory_context={"mode":mode,"sre_tracker_enabled":mode in {"both","sre"},"rhokp_enabled":mode in {"both","rhokp"}}
        if mode in {"both","sre"} and self.tracker:
            try:
                failure_class=self._failure_class(state.classification)
                current_id=str(state.failure_case_id)
                rows=self.tracker.search_failure_history(failure_class=failure_class, cluster_id=state.event.get('cluster_id'), limit=20)
                state.historical_context=[r for r in rows if str(r.get('failure_case_id','')) != current_id][:10]
                state.periodic_context=self.tracker.search_periodic_patterns(failure_class=failure_class, cluster_id=state.event.get('cluster_id'), limit=10)
                state.trace.append(f'memory SRE loaded history={len(state.historical_context)} periodic={len(state.periodic_context)}')
            except Exception as e: state.errors.append(f'SRE memory retrieval: {e}')
        elif mode in {"both","sre"}:
            state.memory_context['sre_tracker_status']='UNAVAILABLE'
        if mode in {"both","rhokp"} and self.knowledge:
            try:
                state.knowledge_context=self.knowledge.search({'query':f'migration {state.classification} {state.event.get("message","")}', 'top_k':5}).get('documents',[])
                state.trace.append(f'memory RHOKP loaded={len(state.knowledge_context)}')
            except Exception as e: state.errors.append(f'RHOKP memory retrieval: {e}')
        elif mode in {"both","rhokp"}:
            state.memory_context['rhokp_status']='UNAVAILABLE'

        state.memory_context['historical_count']=len(state.historical_context)
        state.memory_context['periodic_count']=len(state.periodic_context)
        state.memory_context['knowledge_count']=len(state.knowledge_context)
