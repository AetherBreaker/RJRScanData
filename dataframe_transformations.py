# sourcery skip: simplify-generator
if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from decimal import Decimal
from itertools import chain
from logging import getLogger
from math import isnan
from re import compile
from string import Template
from typing import Annotated, Any, Callable, Type

from dataframe_utils import (
  NULL_VALUES,
  combine_same_coupons,
  distribute_discount,
  distribute_multipack,
  fix_decimals,
)
from pandas import DataFrame, Series, concat, isna
from reporting_validation_errs import report_errors
from rich.progress import Progress
from sql_querying import CUR_WEEK
from types_column_names import (
  BulkRateCols,
  GSheetsBuydownsCols,
  GSheetsVAPDiscountsCols,
  ItemizedInvoiceCols,
)
from types_custom import (
  AddressInfoType,
  BulkDataPackage,
  BulkRateDataType,
  BuydownsDataType,
  DeptIDsEnum,
  ItemizedDataPackage,
  ItemizedInvoiceDataType,
  ModelContextType,
  StoreNum,
  VAPDataType,
)
from utils import (
  cached_for_testing,
  convert_storenum_to_str,
  taskgen_whencalled,
  wraps,
)
from validation_config import CustomBaseModel
from validation_itemizedinvoice import ItemizedInvoiceModel
from validators_shared import map_to_upca

logger = getLogger(__name__)


base_context = {
  "skip_fields": {"Dept_ID": None, "Quantity": None},
}

USSTC_BRAND_GROUPS = {
  "Copenhagen Premium": {
    "CAN": [
      "073100001079",
      "073100001216",
      "073100003141",
      "073100002830",
      "073100009624",
    ],
    "ROLL": [
      "073100010897",
      "073100014611",
      "073100014772",
      "073100032837",
      "073100029622",
    ],
  },
  "Copenhagen Popular": {
    "CAN": [
      "073100008764",
      "073100000553",
      "073100000362",
      "073100000393",
      "073100000089",
      "073100000218",
      "073100008825",
      "073100008849",
    ],
    "ROLL": [
      "073100025891",
      "073100030550",
      "073100030369",
      "073100030390",
      "073100030086",
      "073100027215",
      "073100025952",
      "073100025976",
    ],
  },
  "Copenhagen Spit-Free": {
    "CAN": ["073100002649", "073100002632"],
    "ROLL": ["073100012648", "073100012631"],
  },
  "Skoal XTRA": {
    "CAN": [
      "073100001703",
      "073100002724",
      "073100002090",
      "073100002120",
      "073100001673",
      "073100001680",
      "073100002175",
      "073100002137",
    ],
    "ROLL": [
      "073100031700",
      "073100031724",
      "073100032097",
      "073100032127",
      "073100031670",
      "073100031687",
      "073100032172",
      "073100032134",
    ],
  },
  "Skoal Classic": {
    "CAN": [
      "073100001376",
      "073100001482",
      "073100000881",
      "073100000607",
      "073100005412",
      "073100002885",
      "073100003196",
      "073100004575",
      "073100007699",
      "073100001901",
      "073100000904",
    ],
    "ROLL": [
      "073100010934",
      "073100010958",
      "073100010972",
      "073100010996",
      "073100011016",
      "073100011054",
      "073100014789",
      "073100011856",
      "073100023620",
      "073100011948",
      "073100011955",
    ],
  },
  "Skoal Blends": {
    "CAN": [
      "073100002861",
      "073100003134",
      "073100005893",
      "073100004803",
      "073100004804",
      "073100004919",
      "073100005916",
      "073100003448",
      "073100005084",
      "073100009891",
    ],
    "ROLL": [
      "073100011030",
      "073100014802",
      "073100019067",
      "073100012853",
      "073100022142",
      "073100022159",
      "073100019081",
      "073100016783",
      "073100035081",
      "073100090899",
    ],
  },
  "Skoal SNUS": {
    "CAN": ["073100008900", "073100008924"],
    "ROLL": ["073100026027", "073100026041"],
  },
  "Red Seal": {
    "CAN": [
      "073100001857",
      "073100001734",
      "073100001741",
      "073100004551",
      "073100004568",
      "073100001970",
    ],
    "ROLL": [
      "073100011887",
      "073100010651",
      "073100010668",
      "073100011818",
      "073100011849",
      "073100011283",
    ],
  },
  "Husky": {
    "CAN": ["073100001154", "073100001130"],
    "ROLL": ["073100021961", "073100021947", "073100031137"],
  },
}
HELIX_BRAND_GROUPS = [
  "855022005225",
  "855022005287",
  "855022005348",
  "855022005201",
  "855022005263",
  "855022005324",
  "855022005218",
  "855022005270",
  "855022005331",
  "855022005188",
  "855022005249",
  "855022005300",
  "855022005379",
  "855022005386",
  "855022005393",
  "855022005171",
  "855022005232",
  "855022005294",
]


REGULAR_COUPON_DEPARTMENTS = ["Coupon$", "PMPromos", "PromosLT", "PromosST"]


LOYALTY_IDENTIFIERS = {
  "PMUSALoyalty": ["CigsMarl"],
  "PMUSALoyalty1": ["CigsMarl"],
  "HelixLoyaltyIA": ["ChewHelx"],
  "HelixLoyaltyMI": ["ChewHelx"],
  "HelixLoyaltyOH": ["ChewHelx"],
  "HelixLoyaltyWI": ["ChewHelx"],
  "USSTCLoyaltyIA": ["ChewUSST"],
  "USSTCLoyaltyMI": ["ChewUSST"],
  "USSTCLoyaltyOH": ["ChewUSST"],
  "USSTCLoyaltyWI": ["ChewUSST"],
}
MULTIPACK_IDENTIFIERS = {
  "USSTCMultiCanIA": [
    upc
    for upc in chain(
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Popular"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Spit-Free"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal XTRA"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal Classic"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal Blends"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal SNUS"].values())),
    )
  ],
  "USSTCMultipackMI": [
    upc
    for upc in chain(
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Popular"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Spit-Free"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal Blends"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal SNUS"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Red Seal"].values())),
    )
  ],
  "USSTCMultiCanOH": [
    upc
    for upc in chain(
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Popular"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Spit-Free"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal Blends"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Red Seal"].values())),
    )
  ],
  "USSTCMultiCanWI": [
    upc
    for upc in chain(
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Popular"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Copenhagen Spit-Free"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Skoal Blends"].values())),
      chain(*(part for part in USSTC_BRAND_GROUPS["Red Seal"].values())),
    )
  ],
  "HelixMultiCanIA": HELIX_BRAND_GROUPS,
  "HelixMultiCanMI": HELIX_BRAND_GROUPS,
  "HelixMultiCanOH": HELIX_BRAND_GROUPS,
  "HelixMultiCanWI": HELIX_BRAND_GROUPS,
}

LOYALTY_COUPON_DEPARTMENTS = ["PMCOUPON", "HelxCoup", "USSTCoup"]

ALL_COUPON_DEPARTMENTS = REGULAR_COUPON_DEPARTMENTS + LOYALTY_COUPON_DEPARTMENTS

mixnmatch_rate_pattern = Template(r"(?P<Quantity>\d+) ${uom}/\$$(?P<Price>[\d\.]+)")

VALID_MANUFACTURER_MULTIPACK_PATTERNS: dict[tuple[tuple[int, Decimal]], str] = {}

rjr_depts = DeptIDsEnum.rjr_depts()
pm_depts = DeptIDsEnum.pm_depts()
scan_depts = rjr_depts.union(pm_depts)


def context_setup[**P, R](func: Callable[P, R]) -> Callable[P, R]:
  @wraps(func)
  def wrapper(
    row: Series,
    *args: P.args,
    **kwargs: P.kwargs,
  ) -> R:
    context = base_context.copy()

    update = {
      "row_id": row.name,
      "input": row.to_dict(),
      "row_err": {},
    }

    # if key storenum in row
    if ItemizedInvoiceCols.Store_Number in row.index:
      update["store_num"] = row[ItemizedInvoiceCols.Store_Number]

    context.update(update)

    result = func(
      *args,
      **kwargs,
      context=context,
      row=row,
    )

    if context["row_err"]:
      # try:
      report_errors(context)
      # except Exception as e:
      #   logger.error(f"Error reporting errors for row {row.name}: {e}")

    return result

  return wrapper


@context_setup
def apply_model_to_df_transforming(
  context: ModelContextType,
  row: Series,
  new_rows: list[Series],
  model: Type[CustomBaseModel],
) -> Series:
  """
  Apply a model to a row of a DataFrame.
  This version transforms the dataframe and should be passed an empty list to append new rows to
  for concatenation.

  :param context: The context to use.
  :param row: The row to transform.
  :param new_rows: The list of new rows to append to.
  :param model: The model to apply.
  :return: The transformed row.
  """

  context["model"] = model

  # create a new instance of the model
  model = model.model_validate(context["input"], context=context)

  if context["row_err"].get("account_loyalty_id_number", None):
    return row

  if context["row_err"].get("upc_code", None):
    return row

  # serialize the model to a dict
  model_dict = model.model_dump()

  # create a new Series from the model dict
  new_row = Series(model_dict, name=context["row_id"], dtype=object)

  new_rows.append(new_row)

  return row


@context_setup
def apply_model_to_df(
  row: Series,
  context: ModelContextType,
  model: Type[CustomBaseModel],
) -> Series:
  """
  Apply a model to a row of a DataFrame.

  :param context: The context to use.
  :param row: The row to transform.
  :param model: The model to apply.
  :return: The transformed row.
  """

  context["model"] = model

  # create a new instance of the model
  model = model.model_validate(context["input"], context=context)

  # serialize the model to a dict
  model_dict = model.model_dump()

  row.update(model_dict)

  return row


def init_bulk_types(row: Series) -> Series:
  row[BulkRateCols.ItemNum] = str(map_to_upca(row[BulkRateCols.ItemNum]))
  row[BulkRateCols.Bulk_Price] = Decimal(row[BulkRateCols.Bulk_Price])
  row[BulkRateCols.Bulk_Quan] = Decimal(row[BulkRateCols.Bulk_Quan])
  return row


@cached_for_testing(date_for_sig=CUR_WEEK)
def bulk_rate_validation_pass(
  pbar: Annotated[Progress, "ignore_for_sig"],
  storenum: StoreNum,
  bulk_dat: Annotated[BulkRateDataType, "ignore_for_sig"],
) -> BulkDataPackage:
  bulk_dat = bulk_dat.map(fix_decimals)
  bulk_dat = bulk_dat.replace(NULL_VALUES, value=None)

  bulk_dat = bulk_dat.apply(
    taskgen_whencalled(
      pbar,
      description=f"Validating {storenum:0>3} bulk rates",
      total=len(bulk_dat),
      clear_when_finished=True,
      # )(apply_model_to_df)(),
    )(init_bulk_types)(),
    # model=BulkRateModel,
    axis=1,
    result_type="broadcast",
  )
  logger.info(
    f"SFT {storenum:0>3}: [bold orange_red1]Finished[/] validating bulk rates",
    extra={"markup": True},
  )

  return BulkDataPackage(
    storenum=storenum,
    bulk_rate_data=bulk_dat,
  )


@cached_for_testing(date_for_sig=CUR_WEEK)
def itemized_inv_first_validation_pass(
  pbar: Annotated[Progress, "ignore_for_sig"],
  storenum: StoreNum,
  itemized_invoice_data: Annotated[ItemizedInvoiceDataType, "ignore_for_sig"],
  addr_info: Annotated[AddressInfoType, "ignore_for_sig"],
  errors: Annotated[dict[int, list[Any]], "ignore_for_sig"] = None,
) -> ItemizedDataPackage:
  itemized_invoice_data[ItemizedInvoiceCols.Store_Number] = storenum
  itemized_invoice_data[ItemizedInvoiceCols.Store_Name] = convert_storenum_to_str(storenum)

  # filter itemized invoices down to only RJR and PM departments
  itemized_invoice_data = itemized_invoice_data[
    itemized_invoice_data[ItemizedInvoiceCols.Dept_ID].isin(scan_depts)
  ]

  itemized_invoice_data = itemized_invoice_data.map(fix_decimals)
  itemized_invoice_data = itemized_invoice_data.astype(object)
  itemized_invoice_data = itemized_invoice_data.replace(NULL_VALUES, value=None)

  itemized_invoice_data.sort_values(ItemizedInvoiceCols.DateTime, inplace=True)

  series_to_concat = []

  itemized_invoice_data.apply(
    taskgen_whencalled(
      pbar,
      description=f"Validating {storenum:0>3} itemized invoices",
      total=len(itemized_invoice_data),
      clear_when_finished=True,
    )(apply_addrinfo_and_initial_validation)(),
    axis=1,
    model=ItemizedInvoiceModel,
    new_rows=series_to_concat,
    addr_data=addr_info,
  )
  logger.info(
    f"SFT {storenum:0>3}: [bold yellow]Finished[/] validating itemized invoices",
    extra={"markup": True},
  )

  try:
    itemized_invoice_data = concat(series_to_concat, axis=1).T
  except ValueError:
    return storenum

  itemized_invoice_data = itemized_invoice_data[
    itemized_invoice_data[ItemizedInvoiceCols.Dept_ID].isin(DeptIDsEnum.all_columns())
  ]

  return ItemizedDataPackage(
    storenum=storenum,
    itemized_invoice_data=itemized_invoice_data,
  )


@context_setup
def apply_addrinfo_and_initial_validation(
  row: Series,
  context: ModelContextType,
  new_rows: list[Series],
  model: Type[CustomBaseModel],
  addr_data: AddressInfoType,
) -> Series:
  # sourcery skip: remove-empty-nested-block, remove-redundant-if
  """
  Apply a model to a row of a DataFrame.

  :param context: The context to use.
  :param row: The row to transform.
  :param new_rows: The list of new rows to append to.
  :param model: The model to apply.
  :return: The transformed row.
  """

  if row[ItemizedInvoiceCols.CustNum] == "101":
    pass

  context["model"] = model

  address_info = addr_data.loc[context["store_num"]].to_dict()

  context["input"].update(address_info)

  # create a new instance of the model
  model = model.model_validate(context["input"], context=context)

  if quantity_err := context["row_err"].get("Quantity", None):
    if isinstance(quantity_err[0], Decimal):
      return row

  if context["row_err"].get("CustNum", None):
    return row

  if row[ItemizedInvoiceCols.ItemName] == "Cigar Promo 100% Discount":
    return row

  # serialize the model to a dict
  model_dict = model.model_dump()

  new_row = Series(model_dict, name=context["row_id"], dtype=object)

  new_rows.append(new_row)

  return row


def process_item_lines(
  group: DataFrame,
  bulk_rate_data: dict[StoreNum, BulkRateDataType],
  buydowns_data: BuydownsDataType,
  vap_data: VAPDataType,
) -> DataFrame:  # sourcery skip: remove-redundant-if
  if group.empty:
    return group

  # invoicenum = group[ItemizedInvoiceCols.Invoice_Number].iloc[0]

  # if invoicenum in [90198]:
  #   pass

  group = apply_vap(group, vap_data)
  group = apply_buydowns(group, buydowns_data)

  group = calculate_scanned_coupons(group)
  group = identify_bulk_rates(group, bulk_rate_data)
  group = identify_multipack(group)
  group = identify_loyalty(group)

  return group


def apply_vap(group: DataFrame, vap_data: VAPDataType) -> DataFrame:
  for index, row in group.iterrows():
    upc = row[ItemizedInvoiceCols.ItemNum]

    vap_data_match = vap_data.loc[vap_data[GSheetsVAPDiscountsCols.UPC] == upc, :]

    if not vap_data_match.empty:
      assert len(vap_data_match) == 1

      vap_row = vap_data_match.iloc[0]

      vap_amt = vap_row[GSheetsVAPDiscountsCols.Discount_Amt]
      vap_desc = vap_row[GSheetsVAPDiscountsCols.Discount_Type]

      group.loc[index, ItemizedInvoiceCols.Manufacturer_Discount_Amt] = vap_amt
      group.loc[index, ItemizedInvoiceCols.Manufacturer_Promo_Desc] = vap_desc

  return group


def apply_buydowns(group: DataFrame, buydowns_data: BuydownsDataType) -> DataFrame:
  for index, row in group.iterrows():
    # lookup the item by State and UPC in the buydowns data
    state = row[ItemizedInvoiceCols.Store_State]
    upc = row[ItemizedInvoiceCols.ItemNum]

    buydowns_data_match = buydowns_data.loc[
      (buydowns_data[GSheetsBuydownsCols.State] == state)
      & (buydowns_data[GSheetsBuydownsCols.UPC] == upc),
      :,
    ]

    if not buydowns_data_match.empty:
      assert len(buydowns_data_match) == 1

      buydown_row = buydowns_data_match.iloc[0]

      buydown_amt = buydown_row[GSheetsBuydownsCols.Buydown_Amt]

      if buydown_amt is not None:
        buydown_desc = buydown_row[GSheetsBuydownsCols.Buydown_Desc]

        fixed_item_price = row[ItemizedInvoiceCols.Inv_Price] + buydown_amt

        group.loc[index, ItemizedInvoiceCols.Manufacturer_Buydown_Amt] = buydown_amt
        group.loc[index, ItemizedInvoiceCols.Manufacturer_Buydown_Desc] = buydown_desc

        group.loc[index, ItemizedInvoiceCols.Inv_Price] = fixed_item_price

  return group


def calculate_scanned_coupons(group: DataFrame) -> DataFrame:
  # sourcery skip: extract-method

  # grab the dept_id of each row
  dept_ids = group[ItemizedInvoiceCols.Dept_ID]

  is_coupon = dept_ids.isin(REGULAR_COUPON_DEPARTMENTS)

  is_coupon_applicable = ~dept_ids.isin(ALL_COUPON_DEPARTMENTS)

  # if the group is nothing by coupon departments, then this invoice only contained items
  # that don't need to be reported
  if is_coupon.all():
    group.drop(index=group.index, inplace=True)
    return group

  # check if any of the dept_ids are in the COUPON_DEPARTMENTS list
  has_coupon = any(is_coupon)

  # check if the group has multiple lines in a valid coupon department
  # has_multiple_coupons = sum(is_coupon) > 1

  if has_coupon:
    coupon_line_indexes = group.loc[
      is_coupon & ~group.duplicated(ItemizedInvoiceCols.ItemNum, keep="first")
    ].index

    group = combine_same_coupons(group, coupon_line_indexes)

    biggest_coupon_index = group.loc[is_coupon, ItemizedInvoiceCols.Inv_Price].idxmax()

    biggest_coupon_row = group.loc[biggest_coupon_index]

    biggest_coupon_value = biggest_coupon_row[ItemizedInvoiceCols.Inv_Price]
    biggest_coupon_name = biggest_coupon_row[ItemizedInvoiceCols.ItemName]

    group.drop(index=coupon_line_indexes, inplace=True)

    invoice_prices = group.loc[is_coupon_applicable, ItemizedInvoiceCols.Inv_Price]
    invoice_quantities = group.loc[is_coupon_applicable, ItemizedInvoiceCols.Quantity]

    # TODO account for percentage discounts

    distributed_discounts = distribute_discount(
      invoice_prices, invoice_quantities, biggest_coupon_value
    )

    # TODO update to apply to PID coupon after identifying SCANNED coupons
    group.loc[is_coupon_applicable, ItemizedInvoiceCols.Acct_Promo_Name] = biggest_coupon_name
    group.loc[is_coupon_applicable, ItemizedInvoiceCols.Acct_Discount_Amt] = distributed_discounts

  return group


def identify_bulk_rates(
  group: DataFrame, bulk_rate_data: dict[StoreNum, BulkRateDataType]
) -> DataFrame:
  if group.empty:
    return group

  storenum = group[ItemizedInvoiceCols.Store_Number].iloc[0]
  store_bulk_data = bulk_rate_data[storenum]

  for index, row in group.iterrows():
    # check if the itemnum is in the bulk rate data and whether the quantity meets the minimum required for a bulk rate
    itemnum = row[ItemizedInvoiceCols.ItemNum]
    if itemnum in store_bulk_data[BulkRateCols.ItemNum].values:
      bulk_rate_row: Series = store_bulk_data.loc[
        store_bulk_data[BulkRateCols.ItemNum] == itemnum
      ].iloc[0]
      bulk_quan = bulk_rate_row[BulkRateCols.Bulk_Quan]
      bulk_price = bulk_rate_row[BulkRateCols.Bulk_Price]

      bulk_price_per_item = bulk_price / bulk_quan

      item_price = row[ItemizedInvoiceCols.Inv_Price]

      bulk_disc_per_item = item_price - bulk_price_per_item

      quantity = row[ItemizedInvoiceCols.Quantity]

      if quantity >= bulk_quan:
        group.loc[index, ItemizedInvoiceCols.Retail_Multipack_Disc_Amt] = bulk_disc_per_item
        group.loc[index, ItemizedInvoiceCols.Retail_Multipack_Quantity] = bulk_quan

  return group


def identify_multipack(group: DataFrame):
  if group.empty:
    return group

  group[ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Discount_Amt] = None
  group[ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Quantity] = None

  # sourcery skip: move-assign, remove-redundant-if
  for index, row in group.iterrows():
    if mixnmatchrate := row[ItemizedInvoiceCols.MixNMatchRate]:
      uom = row[ItemizedInvoiceCols.Unit_Type] or ""
      if isna(uom):
        uom = ""
      pattern = compile(mixnmatch_rate_pattern.substitute(uom=uom))
      if match := pattern.match(mixnmatchrate):
        multipack_quantity = int(match["Quantity"])
        multipack_price = Decimal(match["Price"])

        if multipack_quantity == 1:
          discount_per_item = row[ItemizedInvoiceCols.Inv_Price] - multipack_price
          if discount_per_item > 0:
            group.loc[index, ItemizedInvoiceCols.Acct_Promo_Name] = "Customer Appreciation"
            group.loc[index, ItemizedInvoiceCols.Acct_Discount_Amt] = discount_per_item
          continue

        multipack_price_per_item = multipack_price / multipack_quantity

        discount_per_item = row[ItemizedInvoiceCols.Inv_Price] - multipack_price_per_item
        if discount_per_item <= 0:
          continue

        # TODO check if this mix n match is a manufacturer multipack or a retailer multipack
        if multipack_desc := VALID_MANUFACTURER_MULTIPACK_PATTERNS.get(
          (multipack_quantity, multipack_price)
        ):
          disc_amt_set_field = ItemizedInvoiceCols.Manufacturer_Multipack_Discount_Amt
          multi_quantity_set_field = ItemizedInvoiceCols.Manufacturer_Multipack_Quantity
          group.loc[index, ItemizedInvoiceCols.Manufacturer_Multipack_Desc] = multipack_desc
        else:
          disc_amt_set_field = ItemizedInvoiceCols.Retail_Multipack_Disc_Amt
          multi_quantity_set_field = ItemizedInvoiceCols.Retail_Multipack_Quantity

        group.loc[index, disc_amt_set_field] = discount_per_item
        group.loc[index, multi_quantity_set_field] = multipack_quantity

  itemnums = group[ItemizedInvoiceCols.ItemNum]

  is_coupon = group[ItemizedInvoiceCols.Dept_ID].isin(ALL_COUPON_DEPARTMENTS)

  is_multiunit_coupon = itemnums.isin(MULTIPACK_IDENTIFIERS.keys())

  # if the group is nothing but coupon departments, then this invoice only contained items
  # that don't need to be reported
  if is_coupon.all():
    group.drop(index=group.index, inplace=True)
    return group

  # check if any of the dept_ids are in the COUPON_DEPARTMENTS list
  has_multiunit_coupon = any(is_multiunit_coupon)

  # check if the group has multiple lines in a valid coupon department
  # has_multiple_coupons = sum(is_coupon) > 1

  if has_multiunit_coupon:
    multiunit_coupon_line_indexes = group.loc[
      is_multiunit_coupon & ~group.duplicated(ItemizedInvoiceCols.ItemNum, keep="first")
    ].index

    for multiunit_coupon_index in multiunit_coupon_line_indexes:
      multiunit_coupon_row: Series = group.loc[multiunit_coupon_index]

      multiunit_coupon_value: Decimal = multiunit_coupon_row[ItemizedInvoiceCols.PricePer]
      multiunit_coupon_itemnum: str = multiunit_coupon_row[ItemizedInvoiceCols.ItemNum]
      multiunit_coupon_code: str = multiunit_coupon_row[ItemizedInvoiceCols.ItemName_Extra]
      multiunit_coupon_quantity: int = multiunit_coupon_row[ItemizedInvoiceCols.Quantity]

      applicable_itemnums = MULTIPACK_IDENTIFIERS.get(multiunit_coupon_itemnum)

      if applicable_itemnums is None:
        logger.error(
          f"Unable to find applicable departments for loyalty coupon {multiunit_coupon_itemnum}"
        )
        continue

      is_multiunit_applicable = itemnums.isin(applicable_itemnums)

      group.drop(index=multiunit_coupon_index, inplace=True)

      group.loc[is_multiunit_applicable, ItemizedInvoiceCols.Manufacturer_Multipack_Desc] = (
        multiunit_coupon_code
      )
      group.loc[
        is_multiunit_applicable, ItemizedInvoiceCols.Manufacturer_Multipack_Discount_Amt
      ] = multiunit_coupon_value / 2
      group.loc[is_multiunit_applicable, ItemizedInvoiceCols.Manufacturer_Multipack_Quantity] = 2

      invoice_applicable_quantities = group.loc[
        is_multiunit_applicable, ItemizedInvoiceCols.Quantity
      ]

      distributed_discounts, distributed_quantities = distribute_multipack(
        invoice_applicable_quantities, multiunit_coupon_value, multiunit_coupon_quantity
      )

      group.loc[is_multiunit_applicable, ItemizedInvoiceCols.Manufacturer_Multipack_Desc] = (
        multiunit_coupon_code
      )
      group.loc[
        is_multiunit_applicable, ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Discount_Amt
      ] = distributed_discounts
      group.loc[
        is_multiunit_applicable, ItemizedInvoiceCols.Altria_Manufacturer_Multipack_Quantity
      ] = distributed_quantities

  return group


def identify_loyalty(group: DataFrame) -> DataFrame:
  # sourcery skip: extract-method

  # if not group.empty and group[ItemizedInvoiceCols.Invoice_Number].iloc[0] in [331473]:
  #   pass

  # grab the dept_id of each row
  dept_ids = group[ItemizedInvoiceCols.Dept_ID]

  itemnums = group[ItemizedInvoiceCols.ItemNum]

  is_coupon = dept_ids.isin(ALL_COUPON_DEPARTMENTS)

  is_loyalty_coupon = itemnums.isin(LOYALTY_IDENTIFIERS.keys())

  # if the group is nothing but coupon departments, then this invoice only contained items
  # that don't need to be reported
  if is_coupon.all():
    group.drop(index=group.index, inplace=True)
    return group

  # check if any of the dept_ids are in the COUPON_DEPARTMENTS list
  has_loyalty_coupon = any(is_loyalty_coupon)

  # check if the group has multiple lines in a valid coupon department
  # has_multiple_coupons = sum(is_coupon) > 1

  if has_loyalty_coupon:
    loyalty_coupon_line_indexes = group.loc[
      is_loyalty_coupon & ~group.duplicated(ItemizedInvoiceCols.ItemNum, keep="first")
    ].index

    for loyalty_coupon_index in loyalty_coupon_line_indexes:
      loyalty_coupon_row = group.loc[loyalty_coupon_index]

      loyalty_coupon_value = loyalty_coupon_row[ItemizedInvoiceCols.Inv_Price]
      loyalty_coupon_itemnum = loyalty_coupon_row[ItemizedInvoiceCols.ItemNum]
      loyalty_coupon_code = loyalty_coupon_row[ItemizedInvoiceCols.ItemName_Extra]

      applicable_depts = LOYALTY_IDENTIFIERS.get(loyalty_coupon_itemnum)

      if applicable_depts is None:
        logger.error(
          f"Unable to find applicable departments for loyalty coupon {loyalty_coupon_itemnum}"
        )
        continue

      is_loyalty_applicable = dept_ids.isin(applicable_depts)

      group.drop(index=loyalty_coupon_index, inplace=True)

      invoice_applicable_prices = group.loc[is_loyalty_applicable, ItemizedInvoiceCols.Inv_Price]
      invoice_applicable_quantities = group.loc[is_loyalty_applicable, ItemizedInvoiceCols.Quantity]

      distributed_discounts = distribute_discount(
        invoice_applicable_prices, invoice_applicable_quantities, loyalty_coupon_value
      )

      group.loc[is_loyalty_applicable, ItemizedInvoiceCols.loyalty_disc_desc] = loyalty_coupon_code
      group.loc[is_loyalty_applicable, ItemizedInvoiceCols.PID_Coupon_Discount_Amt] = (
        distributed_discounts
      )

  return group


# TODO
def apply_outlet_promos(group: DataFrame) -> DataFrame:
  raise NotImplementedError
