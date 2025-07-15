from dataclasses import dataclass
from typing import Literal, Union, Optional

MARKET = Literal['INET_MainMarket', 'INET_FirstNorth', 'Genium_MainMarket', 'Genium_FirstNorth']
PHASE = Literal['Closed', 'Pre-Open', 'Opening Auction', 'Continuous Trading', 'Closing Auction', 'Post-Trade']
VALIDITY_PERIOD = Literal['DAVY', 'GTCV', 'GTTV', 'GTDV', 'GTSV', 'GATV', 'GADV', 'GASV', 'IOCV', 'FOKV']

@dataclass
class OrderbookEvent:
    """RTS Annex Table 2 and 3 Orderbook Event Data Structure"""
    submittingentityid: str
    dea: Literal['true', 'false']
    clientcode: str
    investmentdecisionwithinfirm: str
    execwithinfirm: str
    nonexecutingbroker: str
    tradingcapacity: Literal['DEAL', 'MTCH', 'AOTC']
    liquidityprovisionactivity: Literal['true', 'false']
    dateandtime: str
    validityperiod: Union[VALIDITY_PERIOD, str]
    orderrestriction: str
    validityperiodandtime: Optional[str]
    prioritytimestamp: Optional[str]
    prioritysize: Optional[float]
    seqnum: int
    mic: str
    orderbookcode: str
    financialinstrumentidcode: str
    dateofreceipt: str
    orderidcode: str
    orderevent: str
    ordertype: str
    ordertypeclass: str
    limitprice: Optional[float]
    additionallimitprice: Optional[float]
    stopprice: Optional[float]
    paggedlimitprice: Optional[float]
    transactionprice: Optional[float]
    pricecurrency: Optional[str]
    currencyleg2: Optional[str]
    pricenotation: Literal['MONE', 'PERC', 'YIEL', 'BAPO']
    buysellind: Literal['BUY', 'SELL']
    orderstatus: Union[Literal['ACTI', 'INAC', 'FIRM', 'INDI', 'IMPL', 'ROUT'], str]
    quantitynotation: Literal['UNIT', 'NOML', 'MONE']
    quantitycurrency: Optional[str]
    initialqty: float
    remainingqtyinclhidden: float
    displayedqty: Optional[float]
    tradedquantity: Optional[float]
    minacceptableqty: Optional[float]
    minimumexecutablesize: Optional[float]
    mesfirstexeconly: Optional[Literal['true', 'false']]
    passiveonly: Literal['true', 'false']
    passiveoraggressive: Optional[Literal['PASV', 'AGRE']]
    selfexecutionprevention: Optional[Literal['true', 'false']]
    strategylinkedorderid: Optional[str]
    routingstrategy: Optional[str]
    tradingvenuetransactionidcode: Optional[str]
    tradingphases: Optional[PHASE]
    indicativeauctionpx: Optional[float]
    indicativeauctionvol: Optional[float]