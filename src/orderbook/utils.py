from lxml import etree
import hashlib
import os
import logging

# HASH_SECRET_KEY = secrets.token_hex(16)
# print(f"Hash secret key: {HASH_SECRET_KEY}")
# a09bf31a8152d8b1accc73c646253dc5
HASH_SECRET_KEY = "a09bf31a8152d8b1accc73c646253dc5"

def anonymize_hash(val: str) -> str:
    """
    Returns a last 32 chars of hex SHA-256 digest of HASH_SECRET_KEY ∥ val.
    """
    h = hashlib.sha256()
    h.update(HASH_SECRET_KEY.encode('utf8'))
    h.update(val.encode('utf8'))
    return h.hexdigest().upper()[:32]

def check_create_master_schema():
    if os.path.exists("schemas/master_schema.xsd"):
        xsd_doc = etree.parse("schemas/master_schema.xsd")
        return etree.XMLSchema(xsd_doc)

    os.makedirs("schemas", exist_ok=True)
    master_schema = etree.fromstring(
    """<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema
        xmlns:xs="http://www.w3.org/2001/XMLSchema"
        targetNamespace="urn:iso:std:iso:20022:tech:xsd:head.003.001.01"
        xmlns="urn:iso:std:iso:20022:tech:xsd:head.003.001.01"
        elementFormDefault="qualified">
    <xs:import
        namespace="urn:iso:std:iso:20022:tech:xsd:head.001.001.01"
        schemaLocation="head.001.001.01_ESMAUG_1.0.0.xsd"/>
    <xs:import
        namespace="urn:accenture:auth.anonym.113.001.01"
        schemaLocation="auth.anonym.113.001.01.xsd"/>
    <xs:include schemaLocation="head.003.001.01.xsd"/>

    </xs:schema>
    """.encode('utf-8')
    )
    etree.ElementTree(master_schema).write("schemas/master_schema.xsd", encoding='utf-8', xml_declaration=True, pretty_print=True)
    xsd_doc = etree.parse("schemas/master_schema.xsd")
    return etree.XMLSchema(xsd_doc)

def setup_logs():
    logging.basicConfig(
        level=logging.INFO,
        filename='orderbook_report.log',
        filemode='a',
        format='%(asctime)s %(levelname)s %(name)s: %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S',
    )