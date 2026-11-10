var KEY = 'uk7hees4eeBaetei1Aa8-';

function calculateHmacSha256(data, key) {
    var sha256_HMAC = javax.crypto.Mac.getInstance("HmacSHA256");
    var secret_key = new javax.crypto.spec.SecretKeySpec(key.getBytes("UTF-8"), "HmacSHA256");
    sha256_HMAC.init(secret_key);
    var bytes = sha256_HMAC.doFinal(data.getBytes("UTF-8"));

    var sb = new java.lang.StringBuilder();
    for (var i = 0; i < bytes.length; i++) {
        sb.append(java.lang.Integer.toHexString((bytes[i] & 0xff) + 0x100).substring(1));
    }
    return sb.toString();
}

function process_row(row) {
    if (row.table !== "USER_TAG_USERS") {
        row.suppress();
    } else {
        logger.info('process_row database={}, table={}', row.database, row.table);
        var str = JSON.stringify(row.data.toString())
        if (str) {
            var hashedStr = calculateHmacSha256(str, KEY)
            row.data["hmac"] = hashedStr;
            logger.info('process_row row={}', row.data);
        }
    }
}
