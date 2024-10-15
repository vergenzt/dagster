// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type RepoAssetTableFragment = {
  __typename: 'AssetNode';
  id: string;
  groupName: string;
  changedReasons: Array<Types.ChangeReason>;
  opNames: Array<string>;
  isMaterializable: boolean;
  isObservable: boolean;
  isExecutable: boolean;
  isPartitioned: boolean;
  computeKind: string | null;
  hasMaterializePermission: boolean;
  hasReportRunlessAssetEventPermission: boolean;
  description: string | null;
  jobNames: Array<string>;
  kinds: Array<string>;
  assetKey: {__typename: 'AssetKey'; path: Array<string>};
  partitionDefinition: {
    __typename: 'PartitionDefinition';
    description: string;
    dimensionTypes: Array<{
      __typename: 'DimensionDefinitionType';
      type: Types.PartitionDefinitionType;
      dynamicPartitionsDefinitionName: string | null;
    }>;
  } | null;
  autoMaterializePolicy: {
    __typename: 'AutoMaterializePolicy';
    policyType: Types.AutoMaterializePolicyType;
  } | null;
  automationCondition: {
    __typename: 'AutomationCondition';
    label: string | null;
    expandedLabel: Array<string>;
  } | null;
  owners: Array<
    {__typename: 'TeamAssetOwner'; team: string} | {__typename: 'UserAssetOwner'; email: string}
  >;
  tags: Array<{__typename: 'DefinitionTag'; key: string; value: string}>;
  repository: {
    __typename: 'Repository';
    id: string;
    name: string;
    location: {__typename: 'RepositoryLocation'; id: string; name: string};
  };
};

export type WorkspaceAssetsQueryVariables = Types.Exact<{
  selector: Types.RepositorySelector;
}>;

export type WorkspaceAssetsQuery = {
  __typename: 'Query';
  repositoryOrError:
    | {
        __typename: 'PythonError';
        message: string;
        stack: Array<string>;
        errorChain: Array<{
          __typename: 'ErrorChainLink';
          isExplicitLink: boolean;
          error: {__typename: 'PythonError'; message: string; stack: Array<string>};
        }>;
      }
    | {
        __typename: 'Repository';
        id: string;
        name: string;
        assetNodes: Array<{
          __typename: 'AssetNode';
          id: string;
          groupName: string;
          changedReasons: Array<Types.ChangeReason>;
          opNames: Array<string>;
          isMaterializable: boolean;
          isObservable: boolean;
          isExecutable: boolean;
          isPartitioned: boolean;
          computeKind: string | null;
          hasMaterializePermission: boolean;
          hasReportRunlessAssetEventPermission: boolean;
          description: string | null;
          jobNames: Array<string>;
          kinds: Array<string>;
          assetKey: {__typename: 'AssetKey'; path: Array<string>};
          partitionDefinition: {
            __typename: 'PartitionDefinition';
            description: string;
            dimensionTypes: Array<{
              __typename: 'DimensionDefinitionType';
              type: Types.PartitionDefinitionType;
              dynamicPartitionsDefinitionName: string | null;
            }>;
          } | null;
          autoMaterializePolicy: {
            __typename: 'AutoMaterializePolicy';
            policyType: Types.AutoMaterializePolicyType;
          } | null;
          automationCondition: {
            __typename: 'AutomationCondition';
            label: string | null;
            expandedLabel: Array<string>;
          } | null;
          owners: Array<
            | {__typename: 'TeamAssetOwner'; team: string}
            | {__typename: 'UserAssetOwner'; email: string}
          >;
          tags: Array<{__typename: 'DefinitionTag'; key: string; value: string}>;
          repository: {
            __typename: 'Repository';
            id: string;
            name: string;
            location: {__typename: 'RepositoryLocation'; id: string; name: string};
          };
        }>;
      }
    | {__typename: 'RepositoryNotFoundError'};
};

export const WorkspaceAssetsQueryVersion = '66cc791cdcc9c02c4f6d959b631c1905e306cfa5aead1bb9d03814ab323b0183';
