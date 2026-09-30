# Optional LangGraph integration. The deterministic engine remains the source of truth for safety gates.
try:
    from langgraph.graph import StateGraph, START, END
except ImportError:
    StateGraph=START=END=None

def build_graph(engine):
    if StateGraph is None: return None
    graph=StateGraph(dict)
    graph.add_node('run_engine', lambda s: engine.run(s).to_dict())
    graph.add_edge(START,'run_engine'); graph.add_edge('run_engine',END)
    return graph.compile()
