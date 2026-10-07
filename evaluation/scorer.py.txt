def score(result, hidden_truth):
    diagnosis=result.get('diagnosis',{})
    return {
      'classification_correct': result.get('classification')==hidden_truth,
      'diagnosis_not_overclaimed': diagnosis.get('status')=='INSUFFICIENT_EVIDENCE' or bool(diagnosis.get('statement')),
      'required_evidence_gaps': len(diagnosis.get('missing_required_evidence',[])),
      'fail_closed': result.get('status')=='INSUFFICIENT_EVIDENCE' or diagnosis.get('status')!='INSUFFICIENT_EVIDENCE'
    }
