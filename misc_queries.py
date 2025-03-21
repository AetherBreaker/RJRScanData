if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()
from datetime import datetime

from pandas import concat
from sql_query_builders import build_custnums_query
from sql_querying import query_all_stores_multithreaded
from types_custom import QueryPackage

cols = [
  "CustNum",
  "First_Name",
  "Last_Name",
  "Company",
  "Address_1",
  "Address_2",
  "City",
  "State",
  "Zip_Code",
  "Phone_1",
  "Phone_2",
  # "CC_Type",
  # "CC_Num",
  # "CC_Exp",
  # "Discount_Level",
  # "Discount_Percent",
  # "Acct_Open_Date",
  # "Acct_Close_Date",
  # "Acct_Balance",
  # "Acct_Max_Balance",
  # "Bonus_Plan_Member",
  # "Bonus_Points",
  # "Tax_Exempt",
  # "Member_Exp",
  # "Dirty",
  "Phone_3",
  # "Phone_4",
  "EMail",
  # "County",
  # "Def_SP",
  "CreateDate",
  # "Referral",
  "Birthday",
  # "Last_Birthday_Bonus",
  "Last_Visit",
  # "Require_PONum",
  # "Max_Charge_NumDays",
  # "Max_Charge_Amount",
  # "License_Num",
  # "ID_Last_Checked",
  # "Next_Start_Date",
  # "Checking_AcctNum",
  # "PrintNotes",
  # "Loyalty_Plan_ID",
  # "Tax_Rate_ID",
  # "Bill_To_Name",
  # "Contact_1",
  # "Contact_2",
  # "Terms",
  # "Resale_Num",
  # "Last_Coupon",
  # "Account_Type",
  # "ChargeAtCost",
  # "Disabled",
  # "ImagePath",
  # "License_ExpDate",
  # "TaxID",
  # "SecretCode",
  # "OnlineUserName",
  # "OnlinePassword",
  # "Token",
  # "MaskedCardNumber",
  # "InsertOriginatorId",
  # "UpdateOriginatorId",
  # "UpdateTimestamp",
  # "ModifiedDate",
  # "CreateTimestamp",
  # "Attn",
  # "DueDate",
]

queries = {
  "custnums": QueryPackage(
    build_custnums_query(),
    cols,
  ),
}

results = query_all_stores_multithreaded(queries)

custnums = results["custnums"]

for store_num, custnums_df in custnums.items():
  custnums_df["StoreNum"] = store_num

results = concat(
  list(custnums.values()),
  ignore_index=True,
)

# pattern = compile(r"[\d]{4}-[\d]{2}-[\d]{2}")


def fix_birthday(x: datetime):
  return f"{x.year:04}-{x.month:02}-{x.day:02}" if isinstance(x, datetime) else x


results["Birthday"] = results["Birthday"].map(fix_birthday)


# results["CreateDate"] = to_datetime(results["CreateDate"])

results.sort_values(by=["CustNum", "CreateDate", "StoreNum"], inplace=True)

results.drop_duplicates(
  subset=cols,
  inplace=True,
  keep="first",
)

results.drop_duplicates(
  subset=[
    "CustNum",
    "First_Name",
    "Last_Name",
    "Company",
    "Address_1",
    "Address_2",
    "City",
    "State",
    "Zip_Code",
    "Phone_1",
    "Phone_2",
    "Phone_3",
    "EMail",
    # "CreateDate",
    # "Birthday",
    # "Last_Visit",
  ],
  inplace=True,
  keep="first",
)


results.to_csv("custnums.csv", index=False)
