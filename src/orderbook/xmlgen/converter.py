from lxml import etree
import zipfile
import secrets
import numpy as np
from typing import Optional
from datetime import datetime, timezone
import pandas as pd
import os
from orderbook.utils import anonymize_hash, check_create_master_schema
QName = etree.QName


BIZDATA_NAMESPACE = "urn:iso:std:iso:20022:tech:xsd:head.003.001.01"
BIZDATA_NSMAP = {None: BIZDATA_NAMESPACE}

HEADER_NAMESPACE = "urn:iso:std:iso:20022:tech:xsd:head.001.001.01"
HEADER_NSMAP = {None: HEADER_NAMESPACE}

BOOK_NAMESPACE = "urn:accenture:auth.anonym.113.001.01"
BOOK_NSMAP = {None: BOOK_NAMESPACE}

ENCRYPT_PARAMS = {
    "financialinstrumentidcode": False,
    'orderbookcode': False,
    "clientidcode": True,
    "investmentdecisionwithinfirm": True,
    "execwithinfirm": True,
    "nonexecutingbroker": True,
}

MASTER_SCHEMA = check_create_master_schema()

def create_orderbook_xml(orders: pd.DataFrame) -> etree.ElementTree:
    assert orders.shape[0] > 1, "Orders DataFrame must contain more than one row to create an XML document."
    
    bizdata = etree.Element(QName(BIZDATA_NAMESPACE, "BizData"), nsmap=BIZDATA_NSMAP)
    # header
    header = etree.SubElement(bizdata, QName(BIZDATA_NAMESPACE, "Hdr"))
    app_header = etree.SubElement(header, QName(HEADER_NAMESPACE, "AppHdr"), nsmap=HEADER_NSMAP)

    fr = etree.SubElement(app_header, QName(HEADER_NAMESPACE, "Fr"))
    org_id = etree.SubElement(fr, QName(HEADER_NAMESPACE, "OrgId"))
    org_id_id = etree.SubElement(org_id, QName(HEADER_NAMESPACE, "Id"))
    org_id_id_orgid = etree.SubElement(org_id_id, QName(HEADER_NAMESPACE, "OrgId"))
    org_id_id_orgid_othr = etree.SubElement(org_id_id_orgid, QName(HEADER_NAMESPACE, "Othr"))
    org_id_id_orgid_othr_id = etree.SubElement(org_id_id_orgid_othr, QName(HEADER_NAMESPACE, "Id"))
    org_id_id_orgid_othr_id.text = "LT"
    scheme_name = etree.SubElement(org_id_id_orgid_othr, QName(HEADER_NAMESPACE, "SchmeNm"))
    scheme_name_cd = etree.SubElement(scheme_name, QName(HEADER_NAMESPACE, "Cd"))
    scheme_name_cd.text = "NIDN"

    to = etree.SubElement(app_header, QName(HEADER_NAMESPACE, "To"))
    to_org_id = etree.SubElement(to, QName(HEADER_NAMESPACE, "OrgId"))
    to_org_id_id = etree.SubElement(to_org_id, QName(HEADER_NAMESPACE, "Id"))
    to_org_id_id_orgid = etree.SubElement(to_org_id_id, QName(HEADER_NAMESPACE, "OrgId"))
    to_org_id_id_orgid_othr = etree.SubElement(to_org_id_id_orgid, QName(HEADER_NAMESPACE, "Othr"))
    to_org_id_id_orgid_othr_id = etree.SubElement(to_org_id_id_orgid_othr, QName(HEADER_NAMESPACE, "Id"))
    to_org_id_id_orgid_othr_id.text = "ESMA"
    to_org_id_id_orgid_othr_schme_nm = etree.SubElement(to_org_id_id_orgid_othr, QName(HEADER_NAMESPACE, "SchmeNm"))
    to_org_id_id_orgid_othr_schme_nm_cd = etree.SubElement(to_org_id_id_orgid_othr_schme_nm, QName(HEADER_NAMESPACE, "Cd"))
    to_org_id_id_orgid_othr_schme_nm_cd.text = "NIDN"

    biz_msg_id = etree.SubElement(app_header, QName(HEADER_NAMESPACE, "BizMsgIdr"))
    biz_msg_id.text = "OrderBook"
    msg_def_id = etree.SubElement(app_header, QName(HEADER_NAMESPACE, "MsgDefIdr"))
    msg_def_id.text = "auth.113.001.01"
    cre_dt = etree.SubElement(app_header, QName(HEADER_NAMESPACE, "CreDt"))
    cre_dt.text = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    pyld = etree.SubElement(bizdata, QName(BIZDATA_NAMESPACE, "Pyld"))
    main_doc = etree.SubElement(pyld, QName(BOOK_NAMESPACE, "Document"), nsmap=BOOK_NSMAP)
    ordr_book_rpt = etree.SubElement(main_doc, QName(BOOK_NAMESPACE, "OrdrBookRpt"))
    
    rpt_hdr = etree.SubElement(ordr_book_rpt, QName(BOOK_NAMESPACE, "RptHdr"))
    rptg_ntty = etree.SubElement(rpt_hdr, QName(BOOK_NAMESPACE, "RptgNtty"))
    mkt_id_cd = etree.SubElement(rptg_ntty, QName(BOOK_NAMESPACE, "MktIdCd"))
    mkt_id_cd.text = "XLIT"  # Example MIC code

    rptg_prd = etree.SubElement(rpt_hdr, QName(BOOK_NAMESPACE, "RptgPrd"))
    fr_to_dt = etree.SubElement(rptg_prd, QName(BOOK_NAMESPACE, "FrToDt"))
    fr_dt = etree.SubElement(fr_to_dt, QName(BOOK_NAMESPACE, "FrDt"))
    start_dt, end_dt = pd.to_datetime(orders['dateandtime']).aggregate(lambda x: [x.min(), x.max()])
    fr_dt.text = start_dt.strftime("%Y-%m-%d")
    to_dt = etree.SubElement(fr_to_dt, QName(BOOK_NAMESPACE, "ToDt"))
    to_dt.text = end_dt.strftime("%Y-%m-%d")

    orders_isins = orders['financialinstrumentidcode'].unique()
    for isin in orders_isins:
        el = etree.SubElement(rpt_hdr, QName(BOOK_NAMESPACE, "ISIN"))
        if ENCRYPT_PARAMS.get('financialinstrumentidcode', False):
            el.text = anonymize_hash(isin)
        else:
            el.text = isin

    ordr_rpt = etree.SubElement(ordr_book_rpt, QName(BOOK_NAMESPACE, "OrdrRpt"))
    new = etree.SubElement(ordr_rpt, QName(BOOK_NAMESPACE, "New"))
    rpt_id = etree.SubElement(new, QName(BOOK_NAMESPACE, "RptId"))
    rpt_id.text = secrets.token_bytes(16).hex()

    for order in orders.astype(str).to_dict(orient='records'):
        ordr = etree.SubElement(new, QName(BOOK_NAMESPACE, "Ordr"))
        # OrdrIdData
        ordr_id_data = etree.SubElement(ordr, QName(BOOK_NAMESPACE, "OrdrIdData"))
        ordr_book_id = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "OrdrBookId"))
        if ENCRYPT_PARAMS.get('orderbookcode', False):
            ordr_book_id.text = anonymize_hash(order['orderbookcode'])
        else:
            ordr_book_id.text = order['orderbookcode']
        seq_nb = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "SeqNb"))
        seq_nb.text = order['seqnum']
        if order['prioritytimestamp'] != 'nan':
            prty = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "Prty"))
            prty_tmstmp = etree.SubElement(prty, QName(BOOK_NAMESPACE, "TmStmp"))
            prty_tmstmp.text = order['prioritytimestamp']

        tm_stmp = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "TmStmp"))
        tm_stmp.text = order['dateandtime']
        trad_vn = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "TradVn"))
        trad_vn.text = "XLIT"
        fin_instrm = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "FinInstrm"))
        fin_instrm_id = etree.SubElement(fin_instrm, QName(BOOK_NAMESPACE, "Id"))
        if ENCRYPT_PARAMS.get('financialinstrumentidcode', False):
            fin_instrm_id.text = anonymize_hash(order['financialinstrumentidcode'])
        else:
            fin_instrm_id.text = order['financialinstrumentidcode']
        order_id = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "OrdrId"))
        order_id.text = order['orderidcode']
        dt_rct = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "DtOfRct"))
        dt_rct.text = order['dateofreceipt']
        validity_period = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "VldtyPrd"))
        validity_period_cd = etree.SubElement(validity_period, QName(BOOK_NAMESPACE, "VldtyPrdCd"))
        validity_period_cd.text = order['validityperiod']
        if order['validityperiodandtime'] != 'nan':
            validity_dt = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "VldtyDtTm"))
            validity_dt.text = order['validityperiodandtime']
        event_type = etree.SubElement(ordr_id_data, QName(BOOK_NAMESPACE, "EvtTp"))
        if order['orderevent'] in ['CAME', 'CAMO', 'CHME', 'CHMO', 'EXPI', 'FILL', 'NEWO', 'PARF', 'REMA', 'REMO', 'REMH', 'REME', 'TRIG', 'RFQS', 'RFQR']:
            event_type_cd = etree.SubElement(event_type, QName(BOOK_NAMESPACE, "Cd"))
            event_type_cd.text = order['orderevent']
        else:
            event_type_prtry = etree.SubElement(event_type, QName(BOOK_NAMESPACE, "Prtry"))
            event_type_prtry_id = etree.SubElement(event_type_prtry, QName(BOOK_NAMESPACE, "Id"))
            event_type_prtry_id.text = order['orderevent']
            event_type_prtry_issr = etree.SubElement(event_type_prtry, QName(BOOK_NAMESPACE, "Issr"))
            event_type_prtry_issr.text = "XLIT"

        # AuctnData
        auctn_data = etree.SubElement(ordr, QName(BOOK_NAMESPACE, "AuctnData"))
        trading_phase = etree.SubElement(auctn_data, QName(BOOK_NAMESPACE, "TradgPhs"))
        trading_phase.text = order['tradingphases']
        if order['indicativeauctionprice'] != 'nan':
            ind_auctn_price = etree.SubElement(auctn_data, QName(BOOK_NAMESPACE, "IndctvAuctnPric"))
            mntry_val = etree.SubElement(ind_auctn_price, QName(BOOK_NAMESPACE, "MntryVal"))
            mntry_val_amt = etree.SubElement(mntry_val, QName(BOOK_NAMESPACE, "Amt"), Ccy="EUR")
            mntry_val_amt.text = order['indicativeauctionprice']
        if order['indicativeauctionvolume'] != 'nan':
            ind_auctn_vol = etree.SubElement(auctn_data, QName(BOOK_NAMESPACE, "IndctvAuctnVol"))
            unit = etree.SubElement(ind_auctn_vol, QName(BOOK_NAMESPACE, "Unit"))
            unit.text = order['indicativeauctionvolume']

        # OrdrData
        order_data = etree.SubElement(ordr, QName(BOOK_NAMESPACE, "OrdrData"))
        submitting_entity = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "SubmitgNtty"))
        submitting_entity.text = "XLIT"
        dea = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "DrctElctrncAccs"))
        dea.text = order['dea'].lower()

        client_id = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "ClntId"))
        if len(order['clientidcode']) == 20:
            client_id_lei = etree.SubElement(client_id, QName(BOOK_NAMESPACE, "LEI"))
            if ENCRYPT_PARAMS.get('clientidcode', False):
                client_id_lei.text = anonymize_hash(order['clientidcode'])
            else:
                client_id_lei.text = order['clientidcode']
        else:
            client_id_person = etree.SubElement(client_id, QName(BOOK_NAMESPACE, "Prsn"))
            client_id_person_id = etree.SubElement(client_id_person, QName(BOOK_NAMESPACE, "Id"))
            if ENCRYPT_PARAMS.get('clientidcode', False):
                client_id_person_id.text = anonymize_hash(order['clientidcode'])
            else:
                client_id_person_id.text = order['clientidcode']

        if order['investmentdecisionwithinfirm'] != 'nan':
            investment_decision_person = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "InvstmtDcsnPrsn"))
            investment_decision_person_person = etree.SubElement(investment_decision_person, QName(BOOK_NAMESPACE, "Prsn"))
            investment_decision_person_id = etree.SubElement(investment_decision_person_person, QName(BOOK_NAMESPACE, "Id"))
            if ENCRYPT_PARAMS.get('investmentdecisionwithinfirm', False):
                investment_decision_person_id.text = anonymize_hash(order['investmentdecisionwithinfirm'])
            else:
                investment_decision_person_id.text = order['investmentdecisionwithinfirm']

        if order['execwithinfirm'] != 'nan':
            exec_person = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "ExctgPrsn"))
            if order['execwithinfirm'] == 'NORE':
                exec_person_clnt = etree.SubElement(exec_person, QName(BOOK_NAMESPACE, "Clnt"))
                exec_person_clnt.text = "NORE"
            else:
                exec_person_person = etree.SubElement(exec_person, QName(BOOK_NAMESPACE, "Prsn"))
                exec_person_id = etree.SubElement(exec_person_person, QName(BOOK_NAMESPACE, "Id"))
                if ENCRYPT_PARAMS.get('execwithinfirm', False):
                    exec_person_id.text = anonymize_hash(order['execwithinfirm'])
                else:
                    exec_person_id.text = order['execwithinfirm']
        if order['nonexecutingbroker'] != 'nan':
            nonexec_broker = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "NonExctgBrkr"))
            if ENCRYPT_PARAMS.get('nonexecutingbroker', False):
                nonexec_broker.text = anonymize_hash(order['nonexecutingbroker'])
            else:
                nonexec_broker.text = order['nonexecutingbroker']

        trading_capacity = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "TradgCpcty"))
        trading_capacity.text = order['tradingcapacity']
        lp_activity = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "LqdtyPrvsnActvty"))
        lp_activity.text = order['liquidityprovisionactivity'].lower()

        order_clsf = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "OrdrClssfctn"))
        order_tp = etree.SubElement(order_clsf, QName(BOOK_NAMESPACE, "OrdrTp"))
        order_tp.text = order['ordertype']
        order_tp_cls = etree.SubElement(order_clsf, QName(BOOK_NAMESPACE, "OrdrTpClssfctn"))
        order_tp_cls.text = order['ordertypeclass']

        # OrdrPrics
        order_price = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "OrdrPrics"))
        if order['limitprice'] != 'nan':
            lmt_price = etree.SubElement(order_price, QName(BOOK_NAMESPACE, "LmtPric"))
            lmt_price_mntry_val = etree.SubElement(lmt_price, QName(BOOK_NAMESPACE, "MntryVal"))
            lmt_price_mntry_val_amt = etree.SubElement(lmt_price_mntry_val, QName(BOOK_NAMESPACE, "Amt"), Ccy="EUR")
            lmt_price_mntry_val_amt.text = order['limitprice']
        if order['additionallimitprice'] != 'nan':
            additional_lmt_price = etree.SubElement(order_price, QName(BOOK_NAMESPACE, "AddtlLmtPric"))
            additional_lmt_price_mntry_val = etree.SubElement(additional_lmt_price, QName(BOOK_NAMESPACE, "MntryVal"))
            additional_lmt_price_amt = etree.SubElement(additional_lmt_price_mntry_val, QName(BOOK_NAMESPACE, "Amt"), Ccy="EUR")
            additional_lmt_price_amt.text = order['additionallimitprice']
        if order['stopprice'] != 'nan':
            stop_price = etree.SubElement(order_price, QName(BOOK_NAMESPACE, "StopPric"))
            stop_price_mntry_val = etree.SubElement(stop_price, QName(BOOK_NAMESPACE, "MntryVal"))
            stop_price_mntry_val_amt = etree.SubElement(stop_price_mntry_val, QName(BOOK_NAMESPACE, "Amt"), Ccy="EUR")
            stop_price_mntry_val_amt.text = order['stopprice']
        if order['peggedlimitprice'] != 'nan':
            pegged_limit_price = etree.SubElement(order_price, QName(BOOK_NAMESPACE, "PggdPric"))
            pegged_limit_price_mntry_val = etree.SubElement(pegged_limit_price, QName(BOOK_NAMESPACE, "MntryVal"))
            pegged_limit_price_mntry_val_amt = etree.SubElement(pegged_limit_price_mntry_val, QName(BOOK_NAMESPACE, "Amt"), Ccy="EUR")
            pegged_limit_price_mntry_val_amt.text = order['peggedlimitprice']

        # InstrData
        instr_data = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "InstrData"))
        buy_sell = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "BuySellInd"))
        buy_sell.text = order['buysellind']

        statuses = order['orderstatus'].split(',')
        for s in statuses:
            stat = s.strip()
            if stat in ['FIRM', 'IMPL', 'INDI', 'ROUT']:
                order_status = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "OrdrSts"))
                order_status.text = stat
            if stat in ['ACTI', 'INAC', 'SUSP']:
                order_validity = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "OrdrVldtySts"))
                order_validity.text = stat

        init_qty = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "InitlQty"))
        init_qty_unit = etree.SubElement(init_qty, QName(BOOK_NAMESPACE, "Unit"))
        init_qty_unit.text = order['initialqty']
        rem_qty = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "RmngQty"))
        rem_qty_unit = etree.SubElement(rem_qty, QName(BOOK_NAMESPACE, "Unit"))
        rem_qty_unit.text = order['remainingqtyinclhidden']
        if order['displayedqty'] != 'nan':
            disp_qty = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "DispdQty"))
            disp_qty_unit = etree.SubElement(disp_qty, QName(BOOK_NAMESPACE, "Unit"))
            disp_qty_unit.text = order['displayedqty']
        if order['minacceptableqty'] != 'nan':
            min_acceptable_qty = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "MinAccptblQty"))
            min_acceptable_qty_unit = etree.SubElement(min_acceptable_qty, QName(BOOK_NAMESPACE, "Unit"))
            min_acceptable_qty_unit.text = order['minacceptableqty']
        if order['minimumexecutablesize'] != 'nan':
            min_executable = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "MinExctbl"))
            min_executable_sz = etree.SubElement(min_executable, QName(BOOK_NAMESPACE, "Sz"))
            min_executable_sz.text = order['minimumexecutablesize']
            min_executable_frst_only = etree.SubElement(min_executable_sz, QName(BOOK_NAMESPACE, "FrstExctnOnly"))
            min_executable_frst_only.text = order['mesfirstexeconly']
        if order['passiveonly'] != 'nan':
            passive_only = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "PssvOnlyInd"))
            passive_only.text = order['passiveonly'].lower()
        if order['strategylinkedorderid'] != 'nan':
            self_exec_prevention = etree.SubElement(instr_data, QName(BOOK_NAMESPACE, "SlfExctnPrvntn"))
            self_exec_prevention.text = order['selfexecutionprevention']

        if order['tradedquantity'] != '0':
            tx_data = etree.SubElement(order_data, QName(BOOK_NAMESPACE, "TxData"))
            tx_pric = etree.SubElement(tx_data, QName(BOOK_NAMESPACE, "TxPric"))
            tx_pric_pric = etree.SubElement(tx_pric, QName(BOOK_NAMESPACE, "Pric"))
            tx_pric_pric_mntry_val = etree.SubElement(tx_pric_pric, QName(BOOK_NAMESPACE, "MntryVal"))
            tx_pric_pric_mntry_val_amt = etree.SubElement(tx_pric_pric_mntry_val, QName(BOOK_NAMESPACE, "Amt"), Ccy="EUR")
            tx_pric_pric_mntry_val_amt.text = order['transactionprice']
            traded_qty = etree.SubElement(tx_data, QName(BOOK_NAMESPACE, "TraddQty"))
            traded_qty_unit = etree.SubElement(traded_qty, QName(BOOK_NAMESPACE, "Unit"))
            traded_qty_unit.text = order['tradedquantity']
            if order['passiveoraggressive'] != 'nan':
                pass_or_aggr = etree.SubElement(tx_data, QName(BOOK_NAMESPACE, "PssvOrAggrssvInd"))
                pass_or_aggr.text = order['passiveoraggressive']
    return bizdata


def validate_schema(xml_element: etree.ElementBase) -> bool:
    """
    Validates the XML element against the master schema.

    Parameters
    ----------
    xml_element : etree.ElementBase
        The XML element to validate.

    Returns
    -------
    bool
        True if the XML element is valid according to the master schema, False otherwise.
    """
    schema_valid = MASTER_SCHEMA.validate(xml_element)
    if not schema_valid:
        print("XML is not valid according to the master schema.")
        for error in MASTER_SCHEMA.error_log:
            print(f"Error: {error.message} at line {error.line}, column {error.column}")
    return schema_valid


def split_and_write_xml(
        orders: pd.DataFrame, 
        path: Optional[str] = None,
        ver: Optional[str] = "001", 
        cap: Optional[int] = 250000, 
    ) -> str:
    """
    Splits the orders DataFrame into smaller chunks and writes each chunk to an XML file.
    """
    
    if path is None:
        path = 'xml_output'

    if not os.path.exists(path):
        os.makedirs(path)

    start_dt, end_dt = pd.to_datetime(orders['dateandtime']).aggregate(lambda x: [x.min(), x.max()])
    start_dt_fmt = start_dt.strftime("%Y%m%d")
    end_dt_fmt = end_dt.strftime("%Y%m%d")
    now_dt = datetime.now(timezone.utc).strftime("%y%m%d")

    n_splits = int(np.ceil(orders.shape[0] / cap))
    splits_idxs = np.array_split(orders.index, n_splits)
    for i, s_idx in enumerate(splits_idxs, start=1):
        subset_orders = orders.loc[s_idx]
        xml_tree = create_orderbook_xml(subset_orders)
        schema_valid = validate_schema(xml_tree)
        if not schema_valid:
            print(f"XML is not valid against the schema for split {i}. Skipping writing to file. {end_dt_fmt}")
            continue
        # <Sender>_<FileType>_XESMA_<Key1>_<Key2>_<Document ID>_<Year>
        incr = len(os.listdir(path))  # Ensure the path exists
        OUT_FNAME = f"NCALT_DATORD_XESMA_XLIT-{end_dt_fmt}-{now_dt}-{ver}_{str(i).zfill(2)}Z{str(n_splits).zfill(2)}_{str(incr+1).zfill(6)}_{now_dt[:2]}"
        with zipfile.ZipFile(f"{path}" + OUT_FNAME + '.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
            with zipf.open(OUT_FNAME + ".xml", 'w') as xml_file:
                etree.ElementTree(xml_tree).write(xml_file, encoding='utf-8', xml_declaration=True, pretty_print=True)
        print(f"Written XML file: {OUT_FNAME} with {subset_orders.shape[0]} orders.")
    return OUT_FNAME



#
