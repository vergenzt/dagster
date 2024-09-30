import * as dagre from 'dagre';

import {GroupNode, ModelEdge, ModelGraph, ModelNode} from '../common/ModelGraph';
import {LAYOUT_MARGIN_X} from '../common/conts';
import {Point, Rect} from '../common/types';
import {generateCurvePoints, isAssetNode} from '../common/utils';

/** The margin for the top and bottom side of the layout. */
export const LAYOUT_MARGIN_TOP = 36;

/** The margin for the bottom side of the layout */
export const LAYOUT_MARGIN_BOTTOM = 16;

/** The default height of a node. */
export const DEFAULT_NODE_HEIGHT = 26;

/** Node width for test cases. */
export const NODE_WIDTH_FOR_TEST = 50;

/** A node in dagre. */
export declare interface DagreNode {
  id: string;
  width: number;
  height: number;
  x?: number;
  y?: number;
}

interface LayoutGraph {
  nodes: {[id: string]: DagreNode};
  incomingEdges: {[fromId: string]: string[]};
  outgoingEdges: {[fromId: string]: string[]};
}

/**
 * To manage graph layout related tasks.
 *
 * TODO: distribute this task to multiple workers to improvement performance.
 */
export class GraphLayout {
  dagreGraph!: dagre.graphlib.Graph;

  constructor(private readonly modelGraph: ModelGraph) {
    this.dagreGraph = new dagre.graphlib.Graph();
  }

  /** Lays out the model graph rooted from the given root node.  */
  layout(rootNodeId?: string): Rect {
    // Get the children nodes of the given root node.
    let rootNode: GroupNode | undefined = undefined;
    let nodes: ModelNode[] = [];
    if (rootNodeId == null) {
      nodes = this.modelGraph.rootNodes;
    } else {
      rootNode = this.modelGraph.nodesById[rootNodeId] as GroupNode;
      nodes = (rootNode.childrenIds || []).map((nodeId) => this.modelGraph.nodesById[nodeId]!);
    }

    // Init.
    this.configLayout(this.dagreGraph);

    // Get layout graph.
    const layoutGraph = getLayoutGraph(rootNode?.id || '', nodes, this.modelGraph);

    // Set nodes/edges to dagre.
    for (const id of Object.keys(layoutGraph.nodes)) {
      const dagreNode = layoutGraph.nodes[id]!;
      this.dagreGraph.setNode(id, dagreNode);
    }
    for (const fromNodeId of Object.keys(layoutGraph.outgoingEdges)) {
      for (const toNodeId of layoutGraph.outgoingEdges[fromNodeId]!) {
        this.dagreGraph.setEdge(fromNodeId, toNodeId);
      }
    }

    // Run the layout algorithm.
    dagre.layout(this.dagreGraph);

    // Set the results back to the original model nodes and calculate the bound
    // that contains all the nodes.
    let minX = Number.MAX_VALUE;
    const minY = Number.MAX_VALUE;
    let maxX = Number.NEGATIVE_INFINITY;
    const maxY = Number.NEGATIVE_INFINITY;
    for (const node of nodes) {
      const dagreNode = layoutGraph.nodes[node.id];
      if (!dagreNode) {
        console.warn(`Node "${node.id}" is not in the dagre layout result`);
        continue;
      }
      node.x = (dagreNode.x || 0) - dagreNode.width / 2;
      node.y = (dagreNode.y || 0) - dagreNode.height / 2;
      node.width = dagreNode.width;
      node.height = dagreNode.height;
      node.localOffsetX = 0;
      node.localOffsetY = 0;
    }

    // Expand the bound to include all the edges.
    let minEdgeX = Number.MAX_VALUE;
    let minEdgeY = Number.MAX_VALUE;
    let maxEdgeX = Number.NEGATIVE_INFINITY;
    let maxEdgeY = Number.NEGATIVE_INFINITY;
    const dagreEdgeRefs = this.dagreGraph.edges();
    const edges: ModelEdge[] = [];
    for (const dagreEdge of dagreEdgeRefs) {
      const points = this.dagreGraph.edge(dagreEdge).points as Point[];
      // tslint:disable-next-line:no-any Allow arbitrary types.
      const d3 = (globalThis as any)['d3'];
      // tslint:disable-next-line:no-any Allow arbitrary types.
      const three = (globalThis as any)['THREE'];
      const curvePoints =
        typeof three === 'undefined'
          ? []
          : generateCurvePoints(points, d3['line'], d3['curveMonotoneY'], three);
      const fromNode = this.modelGraph.nodesById[dagreEdge.v];
      const toNode = this.modelGraph.nodesById[dagreEdge.w];
      if (fromNode == null) {
        console.warn(`Edge from node not found: "${dagreEdge.v}"`);
        continue;
      }
      if (toNode == null) {
        console.warn(`Edge to node not found: "${dagreEdge.w}"`);
        continue;
      }
      const edgeId = `${fromNode.id}|${toNode.id}`;
      edges.push({
        id: edgeId,
        fromNodeId: fromNode.id,
        toNodeId: toNode.id,
        points,
        curvePoints,
      });
      for (const point of points) {
        minEdgeX = Math.min(minEdgeX, point.x);
        minEdgeY = Math.min(minEdgeY, point.y);
        maxEdgeX = Math.max(maxEdgeX, point.x);
        maxEdgeY = Math.max(maxEdgeY, point.y);
      }
    }
    this.modelGraph.edgesByGroupNodeIds[rootNodeId || ''] = edges;

    // Offset nodes to take into account of edges going out of the bound of all
    // the nodes.
    if (minEdgeX < minX) {
      for (const node of nodes) {
        node.localOffsetX = Math.max(0, minX - minEdgeX);
      }
    }

    minX = Math.min(minEdgeX, minX);
    maxX = Math.max(maxEdgeX, maxX);

    // Make sure the subgraph width is at least the width of the root node.
    let subgraphFullWidth = maxX - minX + LAYOUT_MARGIN_X * 2;
    if (rootNode) {
      const parentNodeWidth = getNodeWidth(rootNode, this.modelGraph);
      if (subgraphFullWidth < parentNodeWidth) {
        const extraOffsetX = (parentNodeWidth - subgraphFullWidth) / 2;
        for (const node of nodes) {
          if (!node.localOffsetX) {
            node.localOffsetX = 0;
          }
          node.localOffsetX += extraOffsetX;
        }
        subgraphFullWidth = parentNodeWidth;
      }
    }

    return {
      x: minX,
      y: minY,
      width: subgraphFullWidth - LAYOUT_MARGIN_X * 2,
      height: maxY - minY,
    };
  }

  private configLayout(dagreGraph: dagre.graphlib.Graph) {
    // See available configs here:
    // https://github.com/dagrejs/dagre/wiki#configuring-the-layout.
    dagreGraph.setGraph({
      nodesep: 20,
      ranksep: 50,
      edgesep: 20,
      marginx: LAYOUT_MARGIN_X,
      marginy: LAYOUT_MARGIN_TOP,
    });
    // No edge labels.
    dagreGraph.setDefaultEdgeLabel(() => ({}));
  }
}

/** An utility function to get the node width using an offscreen canvas. */
export function getNodeWidth(node: ModelNode, modelGraph: ModelGraph) {
  return 100;
}

/** An utility function to get the node height. */
export function getNodeHeight(node: ModelNode, modelGraph: ModelGraph) {
  return 100;
}

/** Gets a layout graph for the given nodes. */
export function getLayoutGraph(
  rootGroupNodeId: string,
  nodes: ModelNode[],
  modelGraph: ModelGraph,
): LayoutGraph {
  const layoutGraph: LayoutGraph = {
    nodes: {},
    incomingEdges: {},
    outgoingEdges: {},
  };

  // Create layout graph nodes.
  for (const node of nodes) {
    if (isAssetNode(node) && node.hideInLayout) {
      continue;
    }
    const dagreNode: DagreNode = {
      id: node.id,
      width: node.width || getNodeWidth(node, modelGraph),
      height: getNodeHeight(node, modelGraph),
    };
    layoutGraph.nodes[node.id] = dagreNode;
  }

  // Set layout graph edges.
  const curLayoutGraphEdges = modelGraph.layoutGraphEdges[rootGroupNodeId] || {};
  for (const [fromNodeId, toNodeIds] of Object.entries(curLayoutGraphEdges)) {
    for (const toNodeId of Object.keys(toNodeIds)) {
      addLayoutGraphEdge(layoutGraph, fromNodeId, toNodeId);
    }
  }

  return layoutGraph;
}

function addLayoutGraphEdge(layoutGraph: LayoutGraph, fromNodeId: string, toNodeId: string) {
  if (layoutGraph.outgoingEdges[fromNodeId] == null) {
    layoutGraph.outgoingEdges[fromNodeId] = [];
  }
  layoutGraph.outgoingEdges[fromNodeId]!.push(toNodeId);

  if (layoutGraph.incomingEdges[toNodeId] == null) {
    layoutGraph.incomingEdges[toNodeId] = [];
  }
  layoutGraph.incomingEdges[toNodeId]!.push(fromNodeId);
}
