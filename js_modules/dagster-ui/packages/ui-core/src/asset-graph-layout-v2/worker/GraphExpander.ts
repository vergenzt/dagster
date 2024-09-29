import * as dagre from 'dagre';

import {GraphLayout, getNodeHeight, getNodeWidth} from './GraphLayout';
import {GroupNode, ModelGraph} from '../common/ModelGraph';
import {LAYOUT_MARGIN_X} from '../common/conts';
import {Rect} from '../common/types';
import {getDeepestExpandedGroupNodeIds, isGroupNode} from '../common/utils';

/**
 * A class that handles expanding and collapsing group nodes in a model graph.
 */
export class GraphExpander {
  /** This is for testing purpose. */
  readonly dagreGraphs: dagre.graphlib.Graph[] = [];

  constructor(
    private readonly modelGraph: ModelGraph,
    private readonly testMode = false,
  ) {}

  /** Expands the given group node to show its child nodes. */
  expandGroupNode(groupNodeId: string) {
    const groupNode = this.modelGraph.nodesById[groupNodeId];
    if (groupNode && isGroupNode(groupNode)) {
      if (groupNode.expanded) {
        return;
      }
      groupNode.expanded = true;
    }

    // From the given group node, layout its children, grow its size, and
    // continue to do the same for all its ancestors until reaching the root.
    let curGroupNodeId: string | undefined = groupNodeId;
    while (curGroupNodeId != null) {
      const curGroupNode = this.modelGraph.nodesById[curGroupNodeId] as GroupNode;
      if (!curGroupNode) {
        break;
      }
      curGroupNode.expanded = true;

      // Layout children.
      const layout = new GraphLayout(this.modelGraph);
      const rect = layout.layout(curGroupNodeId);
      if (this.testMode) {
        this.dagreGraphs.push(layout.dagreGraph);
      }

      // Grow size.
      const curTargetWidth = rect.width + LAYOUT_MARGIN_X * 2;
      const curTargetHeight = this.getTargetGroupNodeHeight(rect, curGroupNode);
      curGroupNode.width = curTargetWidth;
      curGroupNode.height = curTargetHeight;

      // Continue with parent.
      curGroupNodeId = curGroupNode.parentId;
    }

    // Layout the root level nodes.
    const layout = new GraphLayout(this.modelGraph);
    layout.layout();
    if (this.testMode) {
      this.dagreGraphs.push(layout.dagreGraph);
    }

    // From root, update offsets of all nodes that have x, y set (meaning they
    // have the layout data).
    for (const node of this.modelGraph.rootNodes) {
      if (isGroupNode(node)) {
        this.updateNodeOffset(node);
      }
    }
  }

  /** Expands from the given deepest group nodes back to root. */
  expandFromDeepestGroupNodes(groupNodeIds: string[]) {
    // Get all ancestors from the given group node ids.
    const seenGroupNodeIds = new Set<string>();
    const queue: string[] = [...groupNodeIds];
    while (queue.length > 0) {
      const curGroupNodeId = queue.shift()!;
      if (seenGroupNodeIds.has(curGroupNodeId)) {
        continue;
      }
      seenGroupNodeIds.add(curGroupNodeId);
      const groupNode = this.modelGraph.nodesById[curGroupNodeId] as GroupNode;
      const parentGroupNodeId = groupNode?.parentId;
      if (parentGroupNodeId) {
        queue.push(parentGroupNodeId);
      }
    }

    // Sort them by level in descending order.
    const sortedGroupNodeIds = Array.from(seenGroupNodeIds).sort((a, b) => {
      const nodeA = this.modelGraph.nodesById[a]!;
      const nodeB = this.modelGraph.nodesById[b]!;
      return nodeB.level - nodeA.level;
    });

    // Layout group nodes in this sorted list.
    for (const groupNodeId of sortedGroupNodeIds) {
      const groupNode = this.modelGraph.nodesById[groupNodeId] as GroupNode;
      groupNode.expanded = true;

      // Layout children.
      const layout = new GraphLayout(this.modelGraph);
      const rect = layout.layout(groupNodeId);
      if (this.testMode) {
        this.dagreGraphs.push(layout.dagreGraph);
      }

      // Grow size.
      const curTargetWidth = rect.width + LAYOUT_MARGIN_X * 2;
      const curTargetHeight = this.getTargetGroupNodeHeight(rect, groupNode);
      groupNode.width = curTargetWidth;
      groupNode.height = curTargetHeight;
    }

    // Layout the root level nodes.
    const layout = new GraphLayout(this.modelGraph);
    layout.layout();
    if (this.testMode) {
      this.dagreGraphs.push(layout.dagreGraph);
    }

    // From root, update offsets of all nodes that have x, y set (meaning they
    // have the layout data).
    for (const node of this.modelGraph.rootNodes) {
      if (isGroupNode(node)) {
        this.updateNodeOffset(node);
      }
    }
  }

  /** Expands the graph to reveal the given node. */
  expandToRevealNode(nodeId: string): string[] {
    const node = this.modelGraph.nodesById[nodeId]!;
    const groupNodes: GroupNode[] = [];
    let curNode = node;
    while (true) {
      const nsParent = this.modelGraph.nodesById[curNode.parentId || ''] as GroupNode;
      if (!nsParent) {
        break;
      }
      groupNodes.unshift(nsParent);
      curNode = nsParent;
    }
    for (const groupNode of groupNodes) {
      this.expandGroupNode(groupNode.id);
    }

    const deepestExpandedGroupNodeIds: string[] = [];
    getDeepestExpandedGroupNodeIds(undefined, this.modelGraph, deepestExpandedGroupNodeIds);
    return deepestExpandedGroupNodeIds;
  }

  /** Collapses the given group node to hide all its child nodes. */
  collapseGroupNode(groupNodeId: string): string[] {
    const groupNode = this.modelGraph.nodesById[groupNodeId] as GroupNode;
    if (!groupNode) {
      return [];
    }
    groupNode.expanded = false;
    delete this.modelGraph.edgesByGroupNodeIds[groupNodeId];

    // Shrink size for the current group node.
    groupNode.width = getNodeWidth(groupNode, this.modelGraph);
    groupNode.height = getNodeHeight(groupNode, this.modelGraph);

    // From the given group node's parent, layout, update size, and continue to
    // do the same for all its ancestors until reaching the root.
    let curGroupNodeId: string | undefined = groupNode.parentId;
    while (curGroupNodeId != null) {
      const curGroupNode = this.modelGraph.nodesById[curGroupNodeId] as GroupNode;
      if (!curGroupNode) {
        break;
      }

      // Layout.
      const layout = new GraphLayout(this.modelGraph);
      const rect = layout.layout(curGroupNodeId);
      if (this.testMode) {
        this.dagreGraphs.push(layout.dagreGraph);
      }

      // Shrink size.
      const curTargetWidth = rect.width + LAYOUT_MARGIN_X * 2;
      const curTargetHeight = this.getTargetGroupNodeHeight(rect, curGroupNode);
      curGroupNode.width = curTargetWidth;
      curGroupNode.height = curTargetHeight;

      // Continue with parent.
      curGroupNodeId = curGroupNode.parentId;
    }

    // Layout the root level nodes.
    const layout = new GraphLayout(this.modelGraph);
    layout.layout();
    if (this.testMode) {
      this.dagreGraphs.push(layout.dagreGraph);
    }

    // From root, update offsets of all nodes that have x, y set (meaning they
    // have the layout data).
    for (const node of this.modelGraph.rootNodes) {
      if (isGroupNode(node)) {
        this.updateNodeOffset(node);
      }
    }

    const deepestExpandedGroupNodeIds: string[] = [];
    getDeepestExpandedGroupNodeIds(undefined, this.modelGraph, deepestExpandedGroupNodeIds);
    return deepestExpandedGroupNodeIds;
  }

  /**
   * Uses the current collapse/expand states of the group nodes and re-lays out
   * the entire graph.
   */
  reLayoutGraph(
    targetDeepestGroupNodeIdsToExpand?: string[],
    clearAllExpandStates?: boolean,
  ): string[] {
    let curTargetDeepestGroupNodeIdsToExpand: string[] | undefined =
      targetDeepestGroupNodeIdsToExpand;
    if (!curTargetDeepestGroupNodeIdsToExpand) {
      // Find the deepest group nodes that non of its child group nodes is
      // expanded.
      const deepestExpandedGroupNodeIds: string[] = [];
      this.clearLayoutData(undefined);
      getDeepestExpandedGroupNodeIds(undefined, this.modelGraph, deepestExpandedGroupNodeIds);
      curTargetDeepestGroupNodeIdsToExpand = deepestExpandedGroupNodeIds;
    } else {
      if (clearAllExpandStates) {
        this.clearLayoutData(undefined, true);
      }
    }

    // Expand those nodes one by one.
    if (curTargetDeepestGroupNodeIdsToExpand.length > 0) {
      this.expandFromDeepestGroupNodes(curTargetDeepestGroupNodeIdsToExpand);
    } else {
      const layout = new GraphLayout(this.modelGraph);
      layout.layout();
    }

    return curTargetDeepestGroupNodeIdsToExpand;
  }

  expandAllGroups(): string[] {
    this.clearLayoutData(undefined, true);

    // Find all deepest group nodes.
    const deepestGroupNodeIds = this.modelGraph.nodes
      .filter(
        (node) =>
          isGroupNode(node) &&
          (node.childrenIds || []).filter((id) => isGroupNode(this.modelGraph.nodesById[id]))
            .length === 0,
      )
      .map((node) => node.id);

    // Expand from them.
    if (deepestGroupNodeIds.length > 0) {
      this.expandFromDeepestGroupNodes(deepestGroupNodeIds);
    }

    return deepestGroupNodeIds;
  }

  collapseAllGroup(): string[] {
    this.clearLayoutData(undefined, true);

    // Layout the root level nodes.
    const layout = new GraphLayout(this.modelGraph);
    layout.layout();

    // From root, update offsets of all nodes that have x, y set (meaning they
    // have the layout data).
    for (const node of this.modelGraph.rootNodes) {
      if (isGroupNode(node)) {
        this.updateNodeOffset(node);
      }
    }

    return [];
  }

  private updateNodeOffset(groupNode: GroupNode) {
    for (const nodeId of groupNode.childrenIds || []) {
      const node = this.modelGraph.nodesById[nodeId]!;
      if (node.x != null && node.y != null) {
        node.globalX = (groupNode.x || 0) + (groupNode.globalX || 0) + (node.localOffsetX || 0);
        node.globalY = (groupNode.y || 0) + (groupNode.globalY || 0) + (node.localOffsetY || 0);
      }
      if (isGroupNode(node)) {
        this.updateNodeOffset(node);
      }
    }
  }

  private clearLayoutData(root: GroupNode | undefined, clearAllExpandStates?: boolean) {
    let childrenIds: string[] = [];
    if (root == null) {
      childrenIds = this.modelGraph.rootNodes.map((node) => node.id);
    } else {
      childrenIds = root.childrenIds || [];
    }
    if (clearAllExpandStates && root != null) {
      root.expanded = false;
      delete this.modelGraph.edgesByGroupNodeIds[root.id];
    }
    for (const childNodeId of childrenIds) {
      const childNode = this.modelGraph.nodesById[childNodeId];
      if (!childNode) {
        continue;
      }
      childNode.width = undefined;
      childNode.height = undefined;
      if (isGroupNode(childNode) && childNode.expanded) {
        this.clearLayoutData(childNode, clearAllExpandStates);
      }
    }
  }

  private getTargetGroupNodeHeight(rect: Rect, _groupNode: GroupNode): number {
    return rect.height;
  }
}
