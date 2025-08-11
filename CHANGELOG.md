# Changelog

# Before v0.10.0

- Merge pull request #49 from top-sim/48_ci_actions ([`0d28f68`](https://github.com/top-sim/topsim/commit/0d28f6844ac52dfa883abc9077800e2c45d99742))
- Merge pull request #45 from top-sim/40_simulator_data_results ([`ade4c82`](https://github.com/top-sim/topsim/commit/ade4c8203a040d59c631e58d982ab98df1ea5360))
- Merge pull request #42 from top-sim/18_minimal_json ([`a24ac44`](https://github.com/top-sim/topsim/commit/a24ac44990d3be5d4eb4bfdc565fe622cf6c2063))
- Merge pull request #36 from top-sim/static_batch_provisioning ([`dca8b68`](https://github.com/top-sim/topsim/commit/dca8b68c165608dde3f00743584ba28b87d4a18c))
- Merge pull request #39 from top-sim/issue_38 ([`bd0c7af`](https://github.com/top-sim/topsim/commit/bd0c7af62cef8b998089dfd225fe5debaefdbae3))
- Merge pull request #35 from top-sim/add_experiment_wrapper ([`ec7215e`](https://github.com/top-sim/topsim/commit/ec7215e661c725ef542960c56fb733b9371748f0))
- Merge pull request #34 from top-sim/update_buffer ([`28282a7`](https://github.com/top-sim/topsim/commit/28282a753cb388322ed748aa48326c2818337300))
- Merge pull request #31 from top-sim/addMemoryToTaskRuntime ([`2333845`](https://github.com/top-sim/topsim/commit/23338458cac9326e3e5726f3392081ff280b2c04))
- Merge pull request #30 from top-sim/addMemoryToTaskRuntime ([`4c4133a`](https://github.com/top-sim/topsim/commit/4c4133a1a22fff308728135363c007e71e44731a))
- Merge pull request #29 from top-sim/addMemoryToTaskRuntime ([`eb7ed8d`](https://github.com/top-sim/topsim/commit/eb7ed8d944fb52ca4c35ed2cee0c9b673a0014df))
- Merge pull request #28 from top-sim/update_data_output ([`9aac98e`](https://github.com/top-sim/topsim/commit/9aac98ee6f0c79c0f2f34634efe1cf9a83f481c9))
- Merge pull request #27 from top-sim/refactor_planner ([`076a0b4`](https://github.com/top-sim/topsim/commit/076a0b4d72206d3e326473c666dc328e1f4c5619))
- Merge pull request #26 from top-sim/create_examples ([`13783c2`](https://github.com/top-sim/topsim/commit/13783c288977ff2720bcebf8928105c43f61a578))
- Merge pull request #25 from top-sim/update_timestep_config ([`ba61646`](https://github.com/top-sim/topsim/commit/ba6164685c3f0e95212811b08b7aa279692242bc))
- Merge pull request #24 from top-sim/ska_workflows_transition ([`0d11c01`](https://github.com/top-sim/topsim/commit/0d11c016ef034703b28b0f2a9c4692ddbad81bae))
- Merge pull request #23 from pritchardn/patch-1 ([`fac9c5d`](https://github.com/top-sim/topsim/commit/fac9c5daa462cbacbcb16feb3ba52342e4065b35))
- Merge pull request #21 from top-sim/development ([`7179ca8`](https://github.com/top-sim/topsim/commit/7179ca83a05dfca00a770538887f2aea7d3327c0))
- Merge pull request #12 from top-sim/output_changes ([`24c5895`](https://github.com/top-sim/topsim/commit/24c5895fcdc4693947fa931e86fe8bc3aaea2067))
- Merge pull request #11 from top-sim/config_update ([`47308c5`](https://github.com/top-sim/topsim/commit/47308c550ef89415a15379f14ac7e2f5cc9b9d27))
- Merge remote-tracking branch 'origin/master' ([`3cd68c1`](https://github.com/top-sim/topsim/commit/3cd68c1f9d5f369ac94ec7fd5c00e048d0e40645))
- Merge remote-tracking branch 'origin/master' ([`7d63fbd`](https://github.com/top-sim/topsim/commit/7d63fbdb859349d98fb49d4b5f352bd0796df452))
- Merge remote-tracking branch 'origin/master' ([`6068808`](https://github.com/top-sim/topsim/commit/6068808264d6c54b17afb8768f16e923b3b2b1ee))
- Merge remote-tracking branch 'origin/master' ([`4b106d8`](https://github.com/top-sim/topsim/commit/4b106d833838e9d2eadf57d8fd66ad28a27a11f0))
- Resolved README.md conflicts ([`3a1fc76`](https://github.com/top-sim/topsim/commit/3a1fc76dd195ed754c100546879dc90e244c9b68))

## [0.1.0] 2021-05-13
This version is going to correspond with the first 'public' release and results of TopSim, which was it's demonstration in the 2021 ISC-HPC conference. 

This can be found either at [this commit](https://github.com/top-sim/topsim/commit/d9f43315d83ff814ac5e4b474f9ac8eeab1c0180), or at the [2021-isc-hpc branch](https://github.com/top-sim/topsim/tree/2021-isc-hpc).

Functionality that existed included: 
- Complete simulation run using configuration files and workflows
- Scheduling algorithm integration with SHADOW library
- Data output as `panda` pickles 
- Runtime-delays with tasks
- Test coverage of >80%.