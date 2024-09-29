import {useLayoutEffect, useMemo, useRef} from 'react';

import {
  ProcessGraphRequest,
  UpdateExpandedGroupsRequest,
  WorkerEvent,
  WorkerEventType,
} from './common/WorkerEvents';
import {GraphData} from '../asset-graph/Utils';
import {LayoutAssetGraphOptions} from '../asset-graph/layout';

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
    this.worker = new Worker(new URL('../workers/dagre_layout.worker', import.meta.url));
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
    let resp: WorkerEvent;
    await new Promise((res) => {
      const requestId = data.requestId;
      this.callbacksByRequestId[requestId] = (response: WorkerEvent) => {
        resp = response;
        res(0);
      };
      this.worker.postMessage(data);
    });
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

  useLayoutEffect(() => {
    async function handleUpdate() {
      if (graphId !== previousGraphId.current) {
        worker.processGraph(graphId, graphData, expandedGroups);
        previousGraphId.current = graphId;
      } else {
        worker.updateExpandedGroups(graphId, expandedGroups);
      }
    }
    handleUpdate();
  }, [graphId, worker, expandedGroups, graphData]);

  previousGraphId.current = graphId;
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
