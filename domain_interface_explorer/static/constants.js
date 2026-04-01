export const CELL_WIDTH = 12;
export const ROW_HEIGHT = 18;
export const HEADER_HEIGHT = 42;
export const LABEL_WIDTH = 240;
export const TEXT_FONT = '12px "Iosevka Web", "IBM Plex Mono", monospace';

export const DEFAULT_EMBEDDING_SETTINGS = {
  distance: "overlap",
  perplexity: "auto",
  learningRate: "auto",
  maxIter: 1000,
  earlyExaggeration: 12.0,
};

export const DEFAULT_CLUSTERING_SETTINGS = {
  method: "hierarchical",
  distance: "overlap",
  minClusterSize: 25,
  minSamples: "",
  clusterSelectionEpsilon: 0.25,
  linkage: "average",
  hierarchicalTarget: "distance_threshold",
  nClusters: "",
  distanceThreshold: 0.5,
  hierarchicalMinClusterSize: 10,
};

export const DEFAULT_SELECTION_SETTINGS = {
  minInterfaceSize: 5,
};

export const THREE_TO_ONE = {
  ALA: "A",
  ARG: "R",
  ASN: "N",
  ASP: "D",
  CYS: "C",
  GLN: "Q",
  GLU: "E",
  GLY: "G",
  HIS: "H",
  ILE: "I",
  LEU: "L",
  LYS: "K",
  MET: "M",
  PHE: "F",
  PRO: "P",
  SER: "S",
  THR: "T",
  TRP: "W",
  TYR: "Y",
  VAL: "V",
  SEC: "U",
  PYL: "O",
  ASX: "B",
  GLX: "Z",
  XLE: "J",
  UNK: "X",
};

export const PARTNER_COLOR_PALETTE = [
  "#b4493a",
  "#2b6cb0",
  "#c7871a",
  "#2f855a",
  "#7b5ea7",
  "#b85c8e",
  "#008b8b",
  "#8a5a44",
  "#d94841",
  "#4c78a8",
  "#b279a2",
  "#a0a43c",
  "#e07a1f",
  "#5c8f29",
  "#795548",
  "#2a9d8f",
];

export const CLUSTER_COLOR_PALETTE = [
  "#3f6ba5",
  "#b2553f",
  "#2f7d5c",
  "#9b6bb3",
  "#c18820",
  "#b35b84",
  "#4a8c8c",
  "#7a5d45",
  "#d2663d",
  "#6d8a2b",
  "#456ccf",
  "#9f4d2d",
];
