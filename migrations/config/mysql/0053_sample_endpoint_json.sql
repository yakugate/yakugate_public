-- Sample endpoint rows: populate the JSON columns properly (PO 2026-07-30:
-- "http connector samples are with you, can you put json properly in http
-- endpoints?"). 0052 created the sample vendors' endpoint rows with
-- url/method/policy only; this fills headers/auth/body_template/config so
-- each row is a COMPLETE transport description in the D-087 shape:
--   headers       — templated header map ({vars} resolved from the profile)
--   auth          — how calls to this endpoint authenticate:
--                     {"type":"query"|"bearer"|"session", ...}
--                     type=session references the sibling 'login' endpoint by
--                     name; its TTL matches the profile's session cache
--   body_template — raw request body (SOAP/XML) where the style uses one
--   config        — encoding + templated param map (query/form/json styles)
-- Extraction regexes / ok_match / status maps stay in adapter_config —
-- semantics, not transport (D-087 layer split).

-- Ufone: GET, creds as query params, XML response.
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_ufone'
SET e.auth   = JSON_OBJECT('type','query','params',JSON_OBJECT('id','{id}','password','{password}')),
    e.config = JSON_OBJECT('encoding','query','params',JSON_OBJECT('msisdn','{to}','message','{text}'))
WHERE e.name='send';
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_ufone'
SET e.auth   = JSON_OBJECT('type','query','params',JSON_OBJECT('id','{id}','password','{password}')),
    e.config = JSON_OBJECT('encoding','query','params',JSON_OBJECT('messageId','{msgid}'))
WHERE e.name='poll';

-- Telenor: session login (extracted session var cached 1800s), then GET send/poll.
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_telenor'
SET e.auth   = JSON_OBJECT('type','query','params',JSON_OBJECT('msisdn','{msisdn}','password','{password}')),
    e.config = JSON_OBJECT('encoding','query')
WHERE e.name='login';
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_telenor'
SET e.auth   = JSON_OBJECT('type','session','login_endpoint','login','ttl_sec',1800,'param','session'),
    e.config = JSON_OBJECT('encoding','query','params',JSON_OBJECT('to','{to}','message','{text}','mask','{from}'))
WHERE e.name='send';
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_telenor'
SET e.auth   = JSON_OBJECT('type','session','login_endpoint','login','ttl_sec',1800,'param','session'),
    e.config = JSON_OBJECT('encoding','query','params',JSON_OBJECT('id','{msgid}'))
WHERE e.name='poll';

-- ZONG: SOAP — headers + raw XML envelope bodies.
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_zong'
SET e.headers = JSON_OBJECT('Content-Type','text/xml; charset=utf-8','SOAPAction','urn:QuickSMS'),
    e.body_template = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><QuickSMS><loginId>{loginId}</loginId><loginPassword>{loginPassword}</loginPassword><Dest>{to}</Dest><Mask>{from}</Mask><Message>{text}</Message></QuickSMS></soapenv:Body></soapenv:Envelope>',
    e.config = JSON_OBJECT('encoding','xml')
WHERE e.name='send';
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_zong'
SET e.headers = JSON_OBJECT('Content-Type','text/xml; charset=utf-8','SOAPAction','urn:QueryStatus'),
    e.body_template = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><QueryStatus><TransactionId>{msgid}</TransactionId></QueryStatus></soapenv:Body></soapenv:Envelope>',
    e.config = JSON_OBJECT('encoding','xml')
WHERE e.name='poll';

-- CMT: raw XML POST; credentials inside the body template.
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_cmt'
SET e.headers = JSON_OBJECT('Content-Type','application/xml'),
    e.body_template = '<sms><user>{user}</user><pass>{pass}</pass><to>{to}</to><from>{from}</from><text>{text}</text><clientRef>{msgid}</clientRef></sms>',
    e.config = JSON_OBJECT('encoding','xml')
WHERE e.name='send';

-- Jazz: form-encoded single xmldoc parameter (a form PARAM, not a raw body).
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_jazz'
SET e.headers = JSON_OBJECT('Content-Type','application/x-www-form-urlencoded'),
    e.config = JSON_OBJECT('encoding','form','params',JSON_OBJECT(
      'xmldoc','<SMSRequest><user>{user}</user><password>{password}</password><oa>{from}</oa><da>{to}</da><message>{text}</message></SMSRequest>'))
WHERE e.name='send';

-- v3: REST JSON with bearer auth.
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_v3'
SET e.headers = JSON_OBJECT('Content-Type','application/json'),
    e.auth    = JSON_OBJECT('type','bearer','token','{apikey}'),
    e.config  = JSON_OBJECT('encoding','json','params',JSON_OBJECT('to','{to}','from','{from}','text','{text}','clientRef','{msgid}'))
WHERE e.name='send';
UPDATE http_endpoints e JOIN vendors v ON v.id = e.vendor_id AND v.cid='sample_v3'
SET e.auth   = JSON_OBJECT('type','bearer','token','{apikey}'),
    e.config = JSON_OBJECT('encoding','json')
WHERE e.name='poll';
