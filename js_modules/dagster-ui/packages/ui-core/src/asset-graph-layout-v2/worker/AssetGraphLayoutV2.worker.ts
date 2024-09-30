import {GraphExpander} from './GraphExpander';
import {GraphLayout} from './GraphLayout';
import {GraphProcessor} from './GraphProcessor';
import {GraphData} from '../../asset-graph/Utils';
import {ModelGraph} from '../common/ModelGraph';
import {
  ProcessGraphResponse,
  UpdateExpandedGroupsResponse,
  WorkerEvent,
  WorkerEventType,
} from '../common/WorkerEvents';

const MODEL_GRAPHS_CACHE: Record<string, ModelGraph> = {};

self.addEventListener('message', (event) => {
  const workerEvent = event.data as WorkerEvent;
  console.log({workerEvent});
  switch (workerEvent.eventType) {
    // Handle processing input graph.
    case WorkerEventType.PROCESS_GRAPH_REQ: {
      const modelGraph = handleProcessGraph(
        workerEvent.graphId,
        workerEvent.graph,
        workerEvent.targetDeepestGroupNodeIdsToExpand,
      );
      cacheModelGraph(modelGraph);
      const resp: ProcessGraphResponse = {
        requestId: workerEvent.requestId,
        eventType: WorkerEventType.PROCESS_GRAPH_RESP,
        modelGraph,
        graphId: workerEvent.graphId,
      };
      console.log({resp});
      postMessage(resp);
      break;
    }
    case WorkerEventType.UPDATE_EXPANDED_GROUPS_REQ: {
      const modelGraph = getCachedModelGraph(workerEvent.modelGraphId);
      handleUpdateExpandedGroups(modelGraph, workerEvent.targetDeepestGroupNodeIdsToExpand);
      cacheModelGraph(modelGraph);
      const resp: UpdateExpandedGroupsResponse = {
        requestId: workerEvent.requestId,
        eventType: WorkerEventType.UPDATE_EXPANDED_GROUPS_RESP,
        modelGraph,
        targetDeepestGroupNodeIdsToExpand: workerEvent.targetDeepestGroupNodeIdsToExpand,
      };
      postMessage(resp);
      break;
    }
    default:
      break;
  }
  console.log('done');
});

function handleProcessGraph(
  graphId: string,
  graph: GraphData,
  targetDeepestGroupNodeIdsToExpand: string[],
): ModelGraph {
  let error: string | undefined = undefined;

  // Processes the given input graph `Graph` into a `ModelGraph`.
  const processor = new GraphProcessor(graphId, graph);
  const modelGraph = processor.process();

  // Check nodes with empty ids.
  if (modelGraph.nodesById[''] != null) {
    error =
      'Some nodes have empty strings as ids which will cause layout failures. See console for details.';
    console.warn('Nodes with empty ids', modelGraph.nodesById['']);
  }

  if (!error) {
    if (targetDeepestGroupNodeIdsToExpand.length) {
      handleUpdateExpandedGroups(modelGraph, targetDeepestGroupNodeIdsToExpand);
    } else {
      const layout = new GraphLayout(modelGraph);
      try {
        layout.layout();
      } catch (e) {
        error = `Failed to layout graph: ${e}`;
      }
    }
  }
  return modelGraph;
}

function handleUpdateExpandedGroups(
  modelGraph: ModelGraph,
  targetDeepestGroupNodeIdsToExpand?: string[],
) {
  const expander = new GraphExpander(modelGraph);
  expander.reLayoutGraph(targetDeepestGroupNodeIdsToExpand, true);
}

function cacheModelGraph(modelGraph: ModelGraph) {
  MODEL_GRAPHS_CACHE[modelGraph.id] = modelGraph;
}

function getCachedModelGraph(modelGraphId: string): ModelGraph {
  const cachedModelGraph = MODEL_GRAPHS_CACHE[modelGraphId];
  if (cachedModelGraph == null) {
    throw new Error(`ModelGraph with id "${modelGraphId}" not found"`);
  }
  return cachedModelGraph;
}
