// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type RunRootQueryVariables = Types.Exact<{
  runId: Types.Scalars['ID'];
}>;

export type RunRootQuery = {
  __typename: 'Query';
  pipelineRunOrError:
    | {__typename: 'PythonError'}
    | {
        __typename: 'Run';
        id: string;
        parentPipelineSnapshotId: string | null;
        runConfigYaml: string;
        canTerminate: boolean;
        hasReExecutePermission: boolean;
        hasTerminatePermission: boolean;
        hasDeletePermission: boolean;
        status: Types.RunStatus;
        mode: string;
        rootRunId: string | null;
        parentRunId: string | null;
        pipelineName: string;
        solidSelection: Array<string> | null;
        pipelineSnapshotId: string | null;
        stepKeysToExecute: Array<string> | null;
        updateTime: number | null;
        startTime: number | null;
        endTime: number | null;
        hasConcurrencyKeySlots: boolean;
        repositoryOrigin: {
          __typename: 'RepositoryOrigin';
          id: string;
          repositoryName: string;
          repositoryLocationName: string;
        } | null;
        tags: Array<{__typename: 'PipelineTag'; key: string; value: string}>;
        assetSelection: Array<{__typename: 'AssetKey'; path: Array<string>}> | null;
        assetCheckSelection: Array<{
          __typename: 'AssetCheckKey';
          name: string;
          assetKey: {__typename: 'AssetKey'; path: Array<string>};
        }> | null;
        executionPlan: {
          __typename: 'ExecutionPlan';
          artifactsPersisted: boolean;
          steps: Array<{
            __typename: 'ExecutionStep';
            key: string;
            kind: Types.StepKind;
            inputs: Array<{
              __typename: 'ExecutionStepInput';
              dependsOn: Array<{__typename: 'ExecutionStep'; key: string; kind: Types.StepKind}>;
            }>;
          }>;
        } | null;
        stepStats: Array<{
          __typename: 'RunStepStats';
          stepKey: string;
          status: Types.StepEventStatus | null;
          startTime: number | null;
          endTime: number | null;
          attempts: Array<{
            __typename: 'RunMarker';
            startTime: number | null;
            endTime: number | null;
          }>;
          markers: Array<{
            __typename: 'RunMarker';
            startTime: number | null;
            endTime: number | null;
          }>;
        }>;
      }
    | {__typename: 'RunNotFoundError'};
};
