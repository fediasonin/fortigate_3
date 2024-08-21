fortinet_attacks = {
    "aggregateBy": [],
    "aliases": None,
    "distributeBy": [],
    "groupBy": [],
    "orderBy": [
        {
            "field": "time",
            "sortOrder": "descending"
        }
    ],
    "select": [
            "time",
            "event_src.host",
            "src.ip",
            "src.geo.country",
            "dst.host",
            "dst.ip",
            "dst.geo.country",
            "dst.port",
            "object.state",
            "object.type",
            "text",
            "uuid"
    ],
    "top": None,
    "where": "(event_src.vendor = \"fortinet\") AND (msgid = \"0419016384\")"
}


vpn_mosreg_attacks = {
    "aggregateBy": [],
    "aliases": None,
    "distributeBy": [],
    "groupBy": [],
    "orderBy": [
        {
            "field": "time",
            "sortOrder": "descending"
        }
    ],
    "select": [
            "time",
            "src.ip",
            "assigned_src_ip",
            "subject.name",
            "text",
            "uuid"
    ],
    "top": None,
    "where": "(task_id = 1a25286e-9600-0001-0000-000000000006 and recv_ipv4 = '10.100.3.26' and msgid = '722051')"
}
