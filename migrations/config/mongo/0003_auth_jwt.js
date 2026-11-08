// 0003  Auth = x-api-key + JWT only — CONFIG (MongoDB)
// Add auth_type + JWT verification fields to account docs (schemaless).
db.accounts.updateMany({}, { $set: {
  auth_type: 'apikey', jwt_alg: null, jwt_secret_ref: null, jwt_public_key: null,
  jwks_uri: null, jwt_issuer: null, jwt_audience: null, jwt_sub_claim: 'sub', jwt_leeway_sec: 60
}});
// password_hash retained for Admin Portal login only (Basic removed from Submit API).
