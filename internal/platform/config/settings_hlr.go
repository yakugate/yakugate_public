// The settings of number lookups.
package config

var _ = settings("HLR", []setting{
	{"YAKU_HLR_DIP_URL", "Live number-lookup URL with {msisdn}; unset = data only"},
	{"YAKU_HLR_DIP_TIMEOUT_MS", "Longest wait for one live lookup, ms (default 300)"},
	{"YAKU_HLR_CALL_LOG", "Live lookups keeping the provider's raw call: all, failed, off (all)"},
	{"YAKU_HLR_MAX_INFLIGHT", "Most live lookups at once (default 64)"},
	{"YAKU_HLR_DIP_MCC_FIELD", "Field in the lookup answer holding the country code (mcc)"},
	{"YAKU_HLR_DIP_MNC_FIELD", "Field in the lookup answer holding the network code (mnc)"},
	{"YAKU_HLR_DIP_OK_FIELD", "Field that must be true for a lookup to count; optional"},
	{"YAKU_HLR_DIP_PORTED_FIELD", "Field saying the number moved network; optional"},
})
