"""
Collection of constants tuples for ART module
"""
__author__ = 'Tatiana Korchuganova'

from types import MappingProxyType

# vars
CACHE_TIMEOUT_MINUTES = 15
EOS_PREFIX = 'https://atlas-art-data.web.cern.ch/atlas-art-data/grid-output/'

# dicts
DATETIME_FORMAT = MappingProxyType({
    'default': '%Y-%m-%d',
    'humanized': '%d %b %Y',
    'humanized_short': '%d %b',
})

N_DAYS_MAX = MappingProxyType({
    'test': 1,
    'updatejoblist': 30,
    'stability': 15,
    'errors': 6,
    'other':  6
})

N_DAYS_DEFAULT = MappingProxyType({
    'test': 1,
    'updatejoblist': 30,
    'stability': 8,
    'errors': 1,
    'other':  6
})

TEST_STATUS_INDEX = MappingProxyType({
    'active': 0,
    'failed': 1,
    'finished': 2,
    'succeeded': 3,
})


# tuples
TEST_STATUS = (
    'finished',
    'failed',
    'active',
    'succeeded'
)

GITLAB_PATH_PER_PACKAGE =  MappingProxyType({
    "AthenaMonitoring": "Control/AthenaMonitoring/test",
    "CaloRecGPU": "Calorimeter/CaloRecGPU/test",
    "CampaignsARTTests":"Tools/CampaignsARTTests/test",
    "DerivationFrameworkAnalysisTests": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkAnalysisTests/test",
    "DerivationFrameworkBPhysART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkBPhysART/test",
    "DerivationFrameworkEgammaART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkEgammaART/test",
    "DerivationFrameworkEventAugmentationART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkEventAugmentationART/test",
    "DerivationFrameworkExoticsART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkExoticsART/test",
    "DerivationFrameworkFlavourTagART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkFlavourTagART/test",
    "DerivationFrameworkHDBSART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkHDBSART/test",
    "DerivationFrameworkHIART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkHIART/test",
    "DerivationFrameworkHiggsART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkHiggsART/test",
    "DerivationFrameworkInDetART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkInDetART/test",
    "DerivationFrameworkJetEtMissART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkJetEtMissART/test",
    "DerivationFrameworkLLPART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkLLPART/test",
    "DerivationFrameworkMCTruthART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkMCTruthART/test",
    "DerivationFrameworkMuonsART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkMuonsART/test",
    "DerivationFrameworkNCB": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkNCB/test",
    "DerivationFrameworkPHYS": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkPHYS/test",
    "DerivationFrameworkPhysicsValidationART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkPhysicsValidationART/test",
    "DerivationFrameworkSMART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkSMART/test",
    "DerivationFrameworkSUSYART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkSUSYART/test",
    "DerivationFrameworkTauART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkTauART/test",
    "DerivationFrameworkTileCalART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkTileCalART/test",
    "DerivationFrameworkTopART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkTopART/test",
    "DerivationFrameworkTrainsART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkTrainsART/test",
    "DerivationFrameworkTriggerART": "PhysicsAnalysis/DerivationFramework/DerivationFrameworkART/DerivationFrameworkTriggerART/test",
    "DigitizationTests": "Simulation/Tests/DigitizationTests/test",
    "DigitizationTestsMT": "Simulation/Tests/DigitizationTestsMT/test",
    "DirectIOART": "Tools/DirectIOART/test",
    "EvgenJobTransforms": "Generators/EvgenJobTransforms/test",
    "FastChainPileup": "Simulation/FastSimulation/FastChainPileup/test",
    "FlavourTaggingTests": "PhysicsAnalysis/JetTagging/FlavourTaggingTests/test",
    "Herwig7_i": "Generators/Herwig7_i/test",
    "ISF_Validation": "Simulation/Tests/ISF_Validation/test",
    "ISF_ValidationMT": "Simulation/Tests/ISF_ValidationMT/test",
    "InDetPerformanceRTT": "InnerDetector/InDetValidation/InDetPerformanceRTT/test",
    "InDetPhysValMonitoring": "InnerDetector/InDetValidation/InDetPhysValMonitoring/test",
    "JetValidation": "Reconstruction/Jet/JetValidation/test",
    "MadGraphControl": "Generators/MadGraphControl/test",
    "MooPerformance": "MuonSpectrometer/MuonValidation/MuonRecValidation/MooPerformance/test",
    "MuonGeomRTT": "MuonSpectrometer/MuonValidation/MuonGeomValidation/MuonGeomRTT/test",
    "MuonRecRTT": "MuonSpectrometer/MuonValidation/MuonRecValidation/MuonRecRTT/test",
    "OverlayMonitoringRTT": "Event/EventOverlay/OverlayMonitoringRTT/test",
    "OverlayTests": "Simulation/Tests/OverlayTests/test",
    "OverlayTestsMT": "Simulation/Tests/OverlayTestsMT/test",
    "PFlowTests": "Reconstruction/PFlow/PFlowTests/test",
    "PowhegControl": "Generators/PowhegControl/test",
    "Pythia8_i": "Generators/Pythia8_i/test",
    "RNTupleART": "Tools/RNTupleART/test",
    "RecExRecoTest": "Reconstruction/RecExample/RecExRecoTest/test",
    "RecJobTransformTests": "Reconstruction/RecExample/RecJobTransformTests/test",
    "SUSYTools": "PhysicsAnalysis/SUSYPhys/SUSYTools/test",
    "Sherpa_i": "Generators/Sherpa_i/test",
    "SimCoreTests": "Simulation/Tests/SimCoreTests/test",
    "SimCoreTestsMT": "Simulation/Tests/SimCoreTestsMT/test",
    "SimExoticsTests": "Simulation/Tests/SimExoticsTests/test",
    "Tier0ChainTests": "Tools/Tier0ChainTests/test",
    "TrfTestsART": "Tools/TrfTestsART/test",
    "TrfTestsARTPlots": "Tools/TrfTestsARTPlots/test",
    "TrigAnalysisTest": "Trigger/TrigValidation/TrigAnalysisTest/test",
    "TrigGpuTest": "Trigger/TrigAccel/TrigGpuTest/test",
    "TrigInDetValidation": "Trigger/TrigValidation/TrigInDetValidation/test",
    "TrigP1Test": "Trigger/TrigValidation/TrigP1Test/test",
    "TriggerTest": "Trigger/TrigValidation/TriggerTest/test",
    "egammaValidation": "Reconstruction/egamma/egammaValidation/test"
})

