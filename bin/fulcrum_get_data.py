#!/usr/bin/env python
# hobbes3

import json
import logging
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import rest_wrapped

@rest_wrapped
def fulcrum_api():
    logger.info("Getting forms...")

    fulcrum_url = sr.config["fulcrum"]["url"]
    fulcrum_headers = sr.config["fulcrum"]["headers"]

    data = ""

    for form_id in sr.config["fulcrum"]["forms"]:
        logger.debug("Getting a form.", extra={"form_id": form_id})

        fulcrum_params = {
            "form_id": form_id,
        }

        r = s.get(fulcrum_url, headers=fulcrum_headers, params=fulcrum_params)

        if r and r.text:
            records = r.json()["records"]

            logger.debug("Found records!", extra={"record_count": len(records)})

            for record in records:
                form_values = record["form_values"]
                equipments = form_values.get("627c", [])
                equipment_new = []

                for equipment in equipments:
                    equipment_new.append({
                        "brand": equipment["form_values"].get("a1a9", None),
                        "model": equipment["form_values"].get("e73a", None),
                        "barcode": equipment["form_values"].get("c075", None),
                    })

                record.update({
                    "meta": {
                        "site_id": form_values.get("f93c", None),
                        "site_name": form_values.get("f3c3", None),
                        "address": form_values.get("803c", None),
                        "description": form_values.get("525a", None),
                        "contact_person": form_values.get("698e", None),
                        "phone": form_values.get("f12e", None),
                        "email": form_values.get("c244", None),
                        "note": form_values.get("d879", None),
                        "agency": form_values.get("d035", None),
                        "equipment": equipment_new,
                    }
                })

                event = {
                    "__session_id": sr.session_id,
                    "index": sr.config["fulcrum"]["index"],
                    "sourcetype": "fulcrum_record",
                    "source": __file__,
                    "event": record,
                }

                data += json.dumps(event)

    cribl_url = sr.config["hec"]["url"]
    cribl_headers = sr.config["hec"]["headers"]

    logger.info("Sending data to Splunk...")
    s.post(cribl_url, headers=cribl_headers, data=data)

if __name__ == "__main__":
    logger = logging.getLogger("splunk_rest.splunk_rest")
    s = sr.retry_session()

    fulcrum_api()
