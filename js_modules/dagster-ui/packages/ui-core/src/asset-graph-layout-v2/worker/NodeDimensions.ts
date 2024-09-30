import {LayoutAssetGraphOptions} from './GraphConfig';
import {ASSET_LINK_NAME_MAX_LENGTH} from '../../asset-graph/layout';

export const getAssetLinkDimensions = (label: string, opts: LayoutAssetGraphOptions) => {
  return opts.direction === 'horizontal'
    ? {width: 32 + 7.1 * Math.min(ASSET_LINK_NAME_MAX_LENGTH, label.length), height: 50}
    : {width: 106, height: 50};
};
