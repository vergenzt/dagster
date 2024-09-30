import {useLayoutEffect, useMemo, useRef, useState} from 'react';

import {NodeType} from './common/ModelGraph';
import {
  ProcessGraphRequest,
  ProcessGraphResponse,
  UpdateExpandedGroupsRequest,
  UpdateExpandedGroupsResponse,
  WorkerEvent,
  WorkerEventType,
} from './common/WorkerEvents';
import {GraphData} from '../asset-graph/Utils';
import {
  AssetGraphLayout,
  AssetLayout,
  AssetLayoutEdge,
  GroupLayout,
  LayoutAssetGraphOptions,
} from '../asset-graph/layout';
import {IPoint} from '../graph/common';

class AssetGraphLayoutWorker {
  private requestId: number = 0;
  private callbacksByRequestId: {[key: number]: (response: WorkerEvent) => void} = {};
  private static _instance: AssetGraphLayoutWorker;
  public static getInstance() {
    if (!this._instance) {
      this._instance = new AssetGraphLayoutWorker();
    }
    return this._instance;
  }

  private worker: Worker;
  private constructor() {
    console.log('created worker');
    this.worker = new Worker(new URL('./worker/AssetGraphLayoutV2.worker', import.meta.url));
    this.worker.addEventListener('message', (event) => {
      const data = event.data as WorkerEvent;
      const cb = this.callbacksByRequestId[data.requestId];
      if (!cb) {
        // Handle invalid state;
        return;
      }
      switch (data.eventType) {
        case WorkerEventType.PROCESS_GRAPH_RESP:
        case WorkerEventType.UPDATE_EXPANDED_GROUPS_RESP:
          cb(data);
          return;
      }
    });
  }

  public async processGraph(graphId: string, graph: GraphData, expandedGroups: string[]) {
    const processGraphRequest: ProcessGraphRequest = {
      requestId: this.requestId++,
      eventType: WorkerEventType.PROCESS_GRAPH_REQ,
      graph,
      graphId,
      targetDeepestGroupNodeIdsToExpand: expandedGroups,
    };
    return await this.sendRequest(processGraphRequest);
  }
  public async updateExpandedGroups(graphId: string, expandedGroups: string[]) {
    const updateExpandedGroupsRequest: UpdateExpandedGroupsRequest = {
      requestId: this.requestId++,
      eventType: WorkerEventType.UPDATE_EXPANDED_GROUPS_REQ,
      modelGraphId: graphId,
      targetDeepestGroupNodeIdsToExpand: expandedGroups,
    };
    return await this.sendRequest(updateExpandedGroupsRequest);
  }

  private async sendRequest(data: WorkerEvent) {
    console.log('send request', data);
    let resp: WorkerEvent;
    await new Promise((res) => {
      const requestId = data.requestId;
      this.callbacksByRequestId[requestId] = (response: WorkerEvent) => {
        resp = response;
        res(0);
      };
      console.log('posting', data);
      this.worker.postMessage(data);
    });
    console.log('response', resp!);
    return resp!;
  }
}

export function useAssetGraphLayout(
  graphData: GraphData,
  expandedGroups: string[],
  // TODO: Add horizontal/vertical options
  _opts: LayoutAssetGraphOptions,
) {
  const worker = AssetGraphLayoutWorker.getInstance();
  const graphId = useMemo(() => computeGraphId(graphData), [graphData]);
  const previousGraphId = useRef('');

  const [layout, setLayout] = useState<AssetGraphLayout | null>();
  const [loading, setLoading] = useState(true);

  useLayoutEffect(() => {
    async function handleUpdate() {
      const isNewGraph = graphId !== previousGraphId.current;
      console.log({isNewGraph, graphId});
      previousGraphId.current = graphId;
      setLoading(true);
      let response: ProcessGraphResponse | UpdateExpandedGroupsResponse;
      if (isNewGraph) {
        response = (await worker.processGraph(
          graphId,
          graphData,
          expandedGroups,
        )) as ProcessGraphResponse;
      } else {
        response = (await worker.updateExpandedGroups(
          graphId,
          expandedGroups,
        )) as UpdateExpandedGroupsResponse;
      }
      setLayout(convertToAssetGraphLayout(response));
      setLoading(false);
    }
    handleUpdate();
  }, [graphId, worker, expandedGroups, graphData]);

  return {
    loading,
    async: true,
    layout,
  };
}

function computeGraphId(graphData: GraphData) {
  // Make the cache key deterministic by alphabetically sorting all of the keys since the order
  // of the keys is not guaranteed to be consistent even when the graph hasn't changed.
  function recreateObjectWithKeysSorted(obj: Record<string, Record<string, boolean>>) {
    const newObj: Record<string, Record<string, boolean>> = {};
    Object.keys(obj)
      .sort()
      .forEach((key) => {
        newObj[key] = Object.keys(obj[key]!)
          .sort()
          .reduce(
            (acc, k) => {
              acc[k] = obj[key]![k]!;
              return acc;
            },
            {} as Record<string, boolean>,
          );
      });
    return newObj;
  }

  return simpleHash(
    JSON.stringify({
      downstream: recreateObjectWithKeysSorted(graphData.downstream),
      upstream: recreateObjectWithKeysSorted(graphData.upstream),
      nodes: Object.keys(graphData.nodes)
        .sort()
        .map((key) => graphData.nodes[key]),
    }),
  );
}

function simpleHash(str: string) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0; // Convert to 32bit integer
  }
  return Math.abs(hash).toString(36); // Convert to base36 for a shorter representation
}

function convertToAssetGraphLayout(
  response: ProcessGraphResponse | UpdateExpandedGroupsResponse,
  direction: 'horizontal' | 'vertical' = 'horizontal',
): AssetGraphLayout {
  const graph = response.modelGraph;
  const nodes: {[id: string]: AssetLayout} = {};
  const groups: {[id: string]: GroupLayout} = {};
  const edges: AssetLayoutEdge[] = [];

  let maxWidth = 0;
  let maxHeight = 0;

  // Process nodes
  for (const node of graph.nodes) {
    const {id, width = 0, height = 0} = node;
    const x = node.x ?? node.globalX ?? 0;
    const y = node.y ?? node.globalY ?? 0;

    // Calculate bounds
    const bounds = {
      x: x - width / 2,
      y: y - height / 2,
      width,
      height,
    };

    if (node.nodeType === NodeType.ASSET_NODE) {
      nodes[id] = {id, bounds};
    } else if (node.nodeType === NodeType.GROUP_NODE) {
      groups[id] = {
        id,
        groupName: node.namespace,
        repositoryName: node.repositoryName,
        repositoryLocationName: node.repositoryLocationName,
        bounds,
        expanded: node.expanded,
      };
    }

    maxWidth = Math.max(maxWidth, x + width / 2);
    maxHeight = Math.max(maxHeight, y + height / 2);
  }

  // Process edges
  for (const node of graph.nodes) {
    if (node.nodeType === NodeType.ASSET_NODE) {
      const assetNode = node;
      const fromNode = node;
      const fromX = fromNode.x ?? fromNode.globalX ?? 0;
      const fromY = fromNode.y ?? fromNode.globalY ?? 0;
      const fromWidth = fromNode.width ?? 0;
      const fromHeight = fromNode.height ?? 0;
      const fromId = fromNode.id;

      if (assetNode.outgoingEdges) {
        for (const edge of assetNode.outgoingEdges) {
          const toNodeId = edge.targetNodeId;
          const toNode = graph.nodesById[toNodeId];
          if (toNode) {
            const toX = toNode.x ?? toNode.globalX ?? 0;
            const toY = toNode.y ?? toNode.globalY ?? 0;
            const toWidth = toNode.width ?? 0;
            const toHeight = toNode.height ?? 0;
            const toId = toNode.id;

            // Adjust positions based on direction
            let fromPoint: IPoint;
            let toPoint: IPoint;
            if (direction === 'horizontal') {
              fromPoint = {x: fromX + fromWidth / 2, y: fromY};
              toPoint = {x: toX - toWidth / 2, y: toY};
            } else {
              fromPoint = {x: fromX, y: fromY + fromHeight / 2};
              toPoint = {x: toX, y: toY - toHeight / 2};
            }

            edges.push({
              from: fromPoint,
              fromId,
              to: toPoint,
              toId,
            });
          }
        }
      }
    }
  }

  // Return AssetGraphLayout
  return {
    width: maxWidth,
    height: maxHeight,
    edges,
    nodes,
    groups,
  };
}
