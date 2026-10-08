package harness

import "encoding/json"

type MetricSample struct {
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

// MetricSamples decodes all scalar samples from one JSON /metrics response.
func MetricSamples(body string) ([]MetricSample, bool) {
	var response struct {
		Metrics []MetricSample `json:"metrics"`
	}
	if json.Unmarshal([]byte(body), &response) != nil {
		return nil, false
	}
	return response.Metrics, true
}

// MetricValueFrom reads one metric from the JSON /metrics response. When a
// metric has multiple labeled samples, the first matching sample is returned.
func MetricValueFrom(body, name string) (float64, bool) {
	return MetricValueWithLabels(body, name, nil)
}

// MetricValueWithLabels finds one sample with all of the provided label
// values. Additional labels on the sample are allowed.
func MetricValueWithLabels(body, name string, labels map[string]string) (float64, bool) {
	samples, ok := MetricSamples(body)
	if !ok {
		return 0, false
	}
	for _, sample := range samples {
		if sample.Name != name {
			continue
		}
		match := true
		for key, value := range labels {
			if sample.Labels[key] != value {
				match = false
				break
			}
		}
		if match {
			return sample.Value, true
		}
	}
	return 0, false
}
