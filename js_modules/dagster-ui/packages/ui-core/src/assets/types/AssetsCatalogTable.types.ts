// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type AssetCatalogTableQueryVariables = Types.Exact<{
  cursor?: Types.InputMaybe<Types.Scalars['String']['input']>;
  limit: Types.Scalars['Int']['input'];
}>;

export type AssetCatalogTableQuery = {
  __typename: 'Query';
  assetsOrError:
    | {
        __typename: 'AssetConnection';
        cursor: string | null;
        nodes: Array<{
          __typename: 'Asset';
          id: string;
          key: {__typename: 'AssetKey'; path: Array<string>};
          definition: {
            __typename: 'AssetNode';
            id: string;
            changedReasons: Array<Types.ChangeReason>;
            groupName: string;
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
          } | null;
        }>;
      }
    | {
        __typename: 'PythonError';
        message: string;
        stack: Array<string>;
        errorChain: Array<{
          __typename: 'ErrorChainLink';
          isExplicitLink: boolean;
          error: {__typename: 'PythonError'; message: string; stack: Array<string>};
        }>;
      };
};

export type AssetCatalogGroupTableQueryVariables = Types.Exact<{
  group?: Types.InputMaybe<Types.AssetGroupSelector>;
}>;

export type AssetCatalogGroupTableQuery = {
  __typename: 'Query';
  assetNodes: Array<{
    __typename: 'AssetNode';
    id: string;
    changedReasons: Array<Types.ChangeReason>;
    groupName: string;
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
  }>;
};

export type AssetCatalogGroupTableNodeFragment = {
  __typename: 'AssetNode';
  id: string;
  changedReasons: Array<Types.ChangeReason>;
  groupName: string;
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

export const AssetCatalogTableQueryVersion = '34085ac8df88f975f14f998aaccebb770f7744772b64666cfc6a5bdf50171723';

export const AssetCatalogGroupTableQueryVersion = 'd2eb0f7a5af67bbce8f20add4c6b98db97e79d7e139bfc7ee36f04337409614d';
