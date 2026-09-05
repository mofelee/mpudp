package main

import (
	"reflect"
	"strconv"

	"github.com/mofelee/mpudp/config"
)

func mpudpConfigMetadata(cfg config.Config) map[string]any {
	value := reflect.ValueOf(cfg)
	version, protocol := "v1", "datagram"
	if field := mpudpMetadataField(value, "Wire", "Version"); field.Kind() == reflect.String && field.String() != "" {
		version = field.String()
	}
	if field := mpudpMetadataField(value, "Protocol"); field.Kind() == reflect.String && field.String() != "" {
		protocol = field.String()
	}
	aggregation := map[string]any{"enabled": false}
	repair := map[string]any{"enabled": false}
	scheduler := map[string]any{
		"outbound_path_rates_bps": map[string]int64{},
		"inbound_path_rates_bps":  map[string]int64{},
	}
	profile := version
	receiveCap := cfg.Transport.MaxUDPPayload
	if version == "v2" {
		if field := mpudpMetadataField(value, "Aggregation", "Enabled"); field.Kind() == reflect.Bool {
			aggregation["enabled"] = field.Bool()
			if field.Bool() {
				profile = "v2-aggregation"
			}
		}
		for _, field := range []struct{ name, key string }{
			{"MaxDelay", "max_delay_ns"},
			{"MaxRecords", "max_records"},
			{"MaxQueuedDatagrams", "max_queued_datagrams"},
			{"MaxQueuedBytes", "max_queued_bytes"},
			{"MaxGroupBytes", "max_group_bytes"},
		} {
			if number := mpudpMetadataField(value, "Aggregation", field.name); number.CanInt() {
				aggregation[field.key] = number.Int()
			}
		}
		if field := mpudpMetadataField(value, "Repair", "Enabled"); field.Kind() == reflect.Bool {
			repair["enabled"] = field.Bool()
		}
		if field := mpudpMetadataField(value, "Transport", "MaxReceiveUDPPayload"); field.CanInt() {
			receiveCap = int(field.Int())
		}
		scheduler["outbound_path_rates_bps"] = mpudpMetadataRates(mpudpMetadataField(value, "Scheduler", "OutboundPathRatesBPS"))
		scheduler["inbound_path_rates_bps"] = mpudpMetadataRates(mpudpMetadataField(value, "Scheduler", "InboundPathRatesBPS"))
		scheduler["default_path_rate_bps"] = int64(100000000)
	}
	return map[string]any{
		"fec": cfg.FEC, "transport": cfg.Transport, "limits": cfg.Limits, "timers": cfg.Timers,
		"configured_carriers": len(cfg.Carriers),
		"mpudp_profile":       profile,
		"wire_version":        version,
		"protocol":            protocol,
		"aggregation":         aggregation,
		"repair":              repair,
		"scheduler":           scheduler,
		"udp_caps": map[string]any{
			"send_hard_cap":    cfg.Transport.MaxUDPPayload,
			"receive_hard_cap": receiveCap,
		},
	}
}

// Optional config fields are reflected so the probe also builds against v0.1.0.
func mpudpMetadataField(value reflect.Value, names ...string) reflect.Value {
	for _, name := range names {
		if value.Kind() != reflect.Struct {
			return reflect.Value{}
		}
		value = value.FieldByName(name)
	}
	return value
}

func mpudpMetadataRates(value reflect.Value) map[string]int64 {
	rates := map[string]int64{}
	if value.Kind() != reflect.Map {
		return rates
	}
	entries := value.MapRange()
	for entries.Next() {
		key, rate := entries.Key(), entries.Value()
		if key.CanInt() && rate.CanInt() {
			rates[strconv.FormatInt(key.Int(), 10)] = rate.Int()
		}
	}
	return rates
}
