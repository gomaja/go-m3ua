package m3ua

// nationalCongestionEndpointConfig gives unrelated tests an explicit MP0,
// numeric-SCON network. Tests of another method provide their own profile.
func nationalCongestionEndpointConfig(config EndpointConfig) EndpointConfig {
	if config.Congestion == nil {
		config.Congestion = &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 3}}
	}
	return config
}
