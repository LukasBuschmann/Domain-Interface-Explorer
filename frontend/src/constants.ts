export const CELL_WIDTH = 12;
export const ROW_HEIGHT = 18;
export const HEADER_HEIGHT = 42;
export const LABEL_WIDTH = 240;
export const TEXT_FONT = '12px "Iosevka Web", "IBM Plex Mono", monospace';

export const DEFAULT_EMBEDDING_SETTINGS = {
  method: "pca",
  distance: "binary",
  perplexity: "auto",
  learningRate: "auto",
  maxIter: 1000,
  earlyExaggerationIter: 250,
  earlyExaggeration: 12.0,
  neighbors: "approx",
  theta: 0.5,
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

export const DEFAULT_STRUCTURE_DISPLAY_SETTINGS = {
  preset: "soft",
  background: "#fdfcf8",
  cameraMode: "perspective",
  fieldOfView: 45,
  illumination: false,
  ambientOcclusion: true,
  antialiasing: "smaa",
  antialiasSampleLevel: 3,
  occlusionStrength: 0.8,
  shadows: false,
  shadowDistance: 4,
  outline: false,
  outlineScale: 1,
  depthOfField: false,
  dofBlur: 8,
  dofFocusRange: 28,
  fog: false,
  fogIntensity: 18,
  sharpen: false,
  exposure: 1.0,
  ambientIntensity: 0.48,
  lightIntensity: 0.82,
  highlightStrength: 0.42,
  highlightColor: "#f3c14f",
  contextAlpha: 0.24,
  contactOpacity: 0.6,
  contactRadius: 0.06,
  roughness: 0.72,
  metalness: 0,
  bumpiness: 0.08,
  quality: "auto",
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
