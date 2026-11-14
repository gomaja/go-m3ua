package main

import "github.com/gomaja/go-m3ua"

func nationalCongestionEndpointConfig(config m3ua.EndpointConfig) m3ua.EndpointConfig {
	if config.Congestion == nil || config.Congestion.Default.Method == 0 {
		config.Congestion = &m3ua.CongestionConfig{Default: m3ua.CongestionProfile{Method: m3ua.CongestionNationalWithoutPriorities, MaxLevel: 3}}
	}
	return config
}

func nationalCongestionArguments(arguments []string) []string {
	for _, argument := range arguments {
		if len(argument) >= len("-congestion-method") && argument[:len("-congestion-method")] == "-congestion-method" {
			return arguments
		}
	}
	return append([]string{"-congestion-method=national-without-priorities"}, arguments...)
}

func nationalCongestionCommandConfig(config commandConfig) commandConfig {
	if config.CongestionMethod == 0 {
		config.CongestionMethod = m3ua.CongestionNationalWithoutPriorities
	}
	return config
}
