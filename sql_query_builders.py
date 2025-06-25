if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import date, datetime
from logging import getLogger

from config import SETTINGS
from pypika.queries import Database, Query, QueryBuilder, Schema, Table
from utils import rjr_start_end_dates

logger = getLogger(__name__)


_DATABASE_CRESQL = Database("cresql")
_schema: Schema = _DATABASE_CRESQL.dbo
_table_itemized_invoices: Table = _schema.Invoice_Itemized
_table_inventory: Table = _schema.Inventory
_table_inventory_coupon: Table = _schema.Inventory_Coupon
_table_invoice_totals: Table = _schema.Invoice_Totals
_table_inventory_bulk_info: Table = _schema.Inventory_Bulk_Info
_table_customer: Table = _schema.Customer
_table_employee: Table = _schema.Employee


def update_database_name(new_db_name: str) -> None:
  if new_db_name != _schema._parent._name:
    _schema._parent = Database(new_db_name)


def build_itemized_invoice_query(start_date: date | datetime, end_date: date | datetime) -> QueryBuilder:
  """Build a query to retrieve itemized invoices between two dates.

  :param start_date: Start date to filter invoices. Inclusive
  :type start_date: date | datetime
  :param end_date: End date to filter invoices. Exclusive
  :type end_date: date | datetime
  :return: QueryBuilder to retrieve itemized invoices between two dates.
  :rtype: QueryBuilder
  """
  if isinstance(start_date, datetime):
    start_date = start_date.date()
  if isinstance(end_date, datetime):
    end_date = end_date.date()

  return (
    (
      Query.from_(_table_itemized_invoices)
      .left_join(_table_inventory)
      .on(_table_itemized_invoices.ItemNum == _table_inventory.ItemNum)
      .left_join(_table_invoice_totals)
      .on(_table_itemized_invoices.Invoice_Number == _table_invoice_totals.Invoice_Number)
      .left_join(_table_customer)
      .on(_table_invoice_totals.CustNum == _table_customer.CustNum)
      .left_join(_table_inventory_coupon)
      .on(_table_itemized_invoices.ItemNum == _table_inventory_coupon.ItemNum)
    )
    .select(
      _table_itemized_invoices.Invoice_Number,
      _table_invoice_totals.CustNum,
      _table_customer.Phone_1,
      _table_invoice_totals.AgeVerificationMethod,
      _table_invoice_totals.AgeVerification,
      _table_itemized_invoices.LineNum,
      _table_invoice_totals.Cashier_ID,
      _table_invoice_totals.Station_ID,
      _table_itemized_invoices.ItemNum,
      _table_inventory.ItemName,
      _table_inventory.ItemName_Extra,
      _table_itemized_invoices.DiffItemName,
      _table_inventory.Dept_ID,
      _table_inventory.Unit_Type,
      _table_inventory.Unit_Size,
      _table_invoice_totals.DateTime,
      _table_itemized_invoices.Quantity,
      _table_itemized_invoices.CostPer,
      _table_itemized_invoices.PricePer,
      _table_itemized_invoices.Tax1Per,
      _table_inventory.Cost.as_("Inv_Cost"),
      _table_inventory.Price.as_("Inv_Price"),
      _table_inventory.Retail_Price.as_("Inv_Retail_Price"),
      _table_inventory_coupon.Coupon_Flat_Percent,
      # _table_itemized_invoices.Kit_ItemNum,
      # _table_itemized_invoices.Store_ID,
      _table_itemized_invoices.origPricePer,
      # _table_itemized_invoices.Special_Price_Lock,
      _table_itemized_invoices.BulkRate,
      _table_itemized_invoices.SalePricePer,
      # _table_itemized_invoices.KitchenQuantityPrinted,
      _table_itemized_invoices.PricePerBeforeDiscount,
      # _table_itemized_invoices.OrigPriceSetBy,
      _table_itemized_invoices.PriceChangedBy,
      # _table_itemized_invoices.Kit_Override,
      # _table_itemized_invoices.KitTotal
    )
    .where(_table_invoice_totals.DateTime >= start_date)
    .where(_table_invoice_totals.DateTime < end_date)
  )


def build_bulk_info_query() -> QueryBuilder:
  """Build a query to retrieve bulk info for all items.

  :return: QueryBuilder to retrieve bulk info for all items.
  :rtype: QueryBuilder
  """

  return Query.from_(_table_inventory_bulk_info).select(
    _table_inventory_bulk_info.ItemNum,
    # _TABLE_INVENTORY_BULK_INFO.Store_ID,
    _table_inventory_bulk_info.Bulk_Price,
    _table_inventory_bulk_info.Bulk_Quan,
    # _TABLE_INVENTORY_BULK_INFO.Description,
    # _TABLE_INVENTORY_BULK_INFO.Price_Type,
    # _TABLE_INVENTORY_BULK_INFO.InsertOriginatorId,
    # _TABLE_INVENTORY_BULK_INFO.UpdateOriginatorId,
    # _TABLE_INVENTORY_BULK_INFO.UpdateTimestamp,
    # _TABLE_INVENTORY_BULK_INFO.ModifiedDate,
    # _TABLE_INVENTORY_BULK_INFO.CreateDate,
    # _TABLE_INVENTORY_BULK_INFO.CreateTimestamp,
  )


def build_custnums_query() -> QueryBuilder:
  """Build a query to retrieve all customer numbers.

  :return: QueryBuilder to retrieve all customer numbers.
  :rtype: QueryBuilder
  """
  return Query.from_(_table_customer).select(
    _table_customer.CustNum,
    _table_customer.First_Name,
    _table_customer.Last_Name,
    _table_customer.Company,
    _table_customer.Address_1,
    _table_customer.Address_2,
    _table_customer.City,
    _table_customer.State,
    _table_customer.Zip_Code,
    _table_customer.Phone_1,
    _table_customer.Phone_2,
    # _table_customer.CC_Type,
    # _table_customer.CC_Num,
    # _table_customer.CC_Exp,
    # _table_customer.Discount_Level,
    # _table_customer.Discount_Percent,
    # _table_customer.Acct_Open_Date,
    # _table_customer.Acct_Close_Date,
    # _table_customer.Acct_Balance,
    # _table_customer.Acct_Max_Balance,
    # _table_customer.Bonus_Plan_Member,
    # _table_customer.Bonus_Points,
    # _table_customer.Tax_Exempt,
    # _table_customer.Member_Exp,
    # _table_customer.Dirty,
    _table_customer.Phone_3,
    # _table_customer.Phone_4,
    _table_customer.EMail,
    # _table_customer.County,
    # _table_customer.Def_SP,
    _table_customer.CreateDate,
    # _table_customer.Referral,
    _table_customer.Birthday,
    # _table_customer.Last_Birthday_Bonus,
    _table_customer.Last_Visit,
    # _table_customer.Require_PONum,
    # _table_customer.Max_Charge_NumDays,
    # _table_customer.Max_Charge_Amount,
    # _table_customer.License_Num,
    # _table_customer.ID_Last_Checked,
    # _table_customer.Next_Start_Date,
    # _table_customer.Checking_AcctNum,
    # _table_customer.PrintNotes,
    # _table_customer.Loyalty_Plan_ID,
    # _table_customer.Tax_Rate_ID,
    # _table_customer.Bill_To_Name,
    # _table_customer.Contact_1,
    # _table_customer.Contact_2,
    # _table_customer.Terms,
    # _table_customer.Resale_Num,
    # _table_customer.Last_Coupon,
    # _table_customer.Account_Type,
    # _table_customer.ChargeAtCost,
    # _table_customer.Disabled,
    # _table_customer.ImagePath,
    # _table_customer.License_ExpDate,
    # _table_customer.TaxID,
    # _table_customer.SecretCode,
    # _table_customer.OnlineUserName,
    # _table_customer.OnlinePassword,
    # _table_customer.Token,
    # _table_customer.MaskedCardNumber,
    # _table_customer.InsertOriginatorId,
    # _table_customer.UpdateOriginatorId,
    # _table_customer.UpdateTimestamp,
    # _table_customer.ModifiedDate,
    # _table_customer.CreateTimestamp,
    # _table_customer.Attn,
    # _table_customer.DueDate,
  )


def build_inventory_data_query() -> QueryBuilder:
  """Build a query to retrieve all inventory data.

  :return: QueryBuilder to retrieve all inventory data.
  :rtype: QueryBuilder
  """
  return Query.from_(_table_inventory).select(
    _table_inventory.ItemNum,
    _table_inventory.ItemName,
    _table_inventory.Cost,
    _table_inventory.Price,
    _table_inventory.Retail_Price,
    _table_inventory.In_Stock,
    _table_inventory.Dept_ID,
    _table_inventory.ItemName_Extra,
  )


def build_volume_report_query(start_date: date | datetime, end_date: date | datetime) -> QueryBuilder:
  """Build a query to retrieve itemized invoices between two dates.

  :param start_date: Start date to filter invoices. Inclusive
  :type start_date: date | datetime
  :param end_date: End date to filter invoices. Exclusive
  :type end_date: date | datetime
  :return: QueryBuilder to retrieve itemized invoices between two dates.
  :rtype: QueryBuilder
  """
  if isinstance(start_date, datetime):
    start_date = start_date.date()
  if isinstance(end_date, datetime):
    end_date = end_date.date()

  return (
    (
      Query.from_(_table_itemized_invoices)
      .left_join(_table_inventory)
      .on(_table_itemized_invoices.ItemNum == _table_inventory.ItemNum)
      .left_join(_table_invoice_totals)
      .on(_table_itemized_invoices.Invoice_Number == _table_invoice_totals.Invoice_Number)
      .left_join(_table_customer)
      .on(_table_invoice_totals.CustNum == _table_customer.CustNum)
      .left_join(_table_inventory_coupon)
      .on(_table_itemized_invoices.ItemNum == _table_inventory_coupon.ItemNum)
    )
    .select(
      _table_itemized_invoices.Invoice_Number,
      _table_itemized_invoices.ItemNum,
      _table_inventory.ItemName,
      _table_inventory.Dept_ID,
      _table_invoice_totals.DateTime,
      _table_itemized_invoices.Quantity,
    )
    .where(_table_invoice_totals.DateTime >= start_date)
    .where(_table_invoice_totals.DateTime < end_date)
  )


def build_employee_info_query() -> QueryBuilder:
  """Build a query to retrieve employee info.

  :return: QueryBuilder to retrieve employee info.
  :rtype: QueryBuilder
  """
  return Query.from_(_table_employee).select(
    _table_itemized_invoices.Cashier_ID,
    _table_itemized_invoices.EmpName,
  )
