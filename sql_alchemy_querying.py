if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from decimal import Decimal
from logging import getLogger

from sqlalchemy import (
  DECIMAL,
  BigInteger,
  Column,
  ForeignKey,
  Integer,
  MetaData,
  String,
)
from sqlalchemy.dialects import mssql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logger = getLogger(__name__)

database = "cresql"


cresql_metadata = MetaData(schema="dbo")


class Base(DeclarativeBase):
  metadata = cresql_metadata
  type_annotation_map = {
    Decimal: DECIMAL.with_variant(mssql.DECIMAL(precision=25, scale=8), "mssql"),
  }


class Inventory(Base):
  __tablename__ = "Inventory"
  ItemNum = Column(mssql.NVARCHAR(length=20), primary_key=True, nullable=False)
  ItemName = Column(mssql.NVARCHAR(length=30), nullable=False)
  Store_ID = Column(mssql.NVARCHAR(length=10), primary_key=True, nullable=False)
  ItemName_Extra = Column(String)
  Dept_ID = Column(String)


class InvoiceTotals(Base):
  __tablename__ = "Invoice_Totals"
  Invoice_Number = Column(mssql.BIGINT, primary_key=True, nullable=False)
  Store_ID = Column(mssql.NVARCHAR(length=10), primary_key=True, nullable=False)
  CustNum = Column(mssql.NVARCHAR(length=12), nullable=False)
  DateTime = Column(Integer, nullable=True)
  ...
  Cashier_ID = Column(mssql.NVARCHAR(length=10), nullable=False)
  Station_ID = Column(mssql.NVARCHAR(length=10), nullable=False)
  ...
  Zip_Code = Column(mssql.NVARCHAR(length=10), nullable=True)
  ...
  AgeVerificationMethod = Column(mssql.INTEGER, nullable=False)
  AgeVerification = Column(mssql.INTEGER, nullable=False)


class InvoiceItemized(Base):
  __tablename__ = "Invoice_Itemized"
  Invoice_Number = Column(
    BigInteger, ForeignKey(InvoiceTotals.Invoice_Number), primary_key=True, nullable=False
  )
  LineNum = Column(mssql.INTEGER, primary_key=True, nullable=False)
  ItemNum = Column(mssql.NVARCHAR(length=20), nullable=False)
  Quantity: Mapped[Decimal] = mapped_column(nullable=False)
  CostPer: Mapped[Decimal] = mapped_column(nullable=False)
  PricePer: Mapped[Decimal] = mapped_column(nullable=False)
  Tax1Per: Mapped[Decimal] = mapped_column(nullable=False)
  # Tax2Per: Mapped[Decimal] = mapped_column(nullable=False)
  # Tax3Per: Mapped[Decimal] = mapped_column(nullable=False)
  # Serial_Num = Column(mssql.BIT, nullable=False)
  # Kit_ItemNum = Column(mssql.NVARCHAR(length=20), nullable=True)
  # BC_Invoice_Number = Column(mssql.INTEGER, nullable=True)
  # LineDisc: Mapped[Decimal] = mapped_column(nullable=True)
  DiffItemName = Column(mssql.NVARCHAR(length=30), nullable=True)
  # NumScans = Column(mssql.INTEGER, nullable=True)
  # numBonus = Column(mssql.INTEGER, nullable=True)
  # Line_Tax_Exempt = Column(mssql.BIT, nullable=False)
  # Commision: Mapped[Decimal] = mapped_column(nullable=True)
  Store_ID = Column(
    mssql.NVARCHAR(length=10), ForeignKey(InvoiceTotals.Store_ID), primary_key=True, nullable=False
  )
  origPricePer: Mapped[Decimal] = mapped_column(nullable=True)
  # Allow_Discounts = Column(mssql.BIT, nullable=False)
  # Person = Column(mssql.NVARCHAR(length=15), nullable=True)
  # Sale_Type = Column(mssql.SMALLINT, nullable=True)
  # Ticket_Number = Column(mssql.NVARCHAR(length=15), nullable=True)
  # IsRental = Column(mssql.BIT, nullable=False)
  # FixedTaxPer: Mapped[Decimal] = mapped_column(nullable=True)
  # GC_Sold = Column(mssql.MONEY, nullable=True)
  # Special_Price_Lock = Column(mssql.BIT, nullable=False)
  # As_Is = Column(mssql.BIT, nullable=False)
  # Returned = Column(mssql.BIT, nullable=False)
  # DOB = Column(mssql.MONEY, nullable=True)
  # UserDefined = Column(mssql.NVARCHAR(length=20), nullable=True)
  # Cashier_ID_Itemized = Column(mssql.NVARCHAR(length=10), nullable=True)
  # IsLayaway = Column(mssql.BIT, nullable=True)
  # ReturnedQuantity: Mapped[Decimal] = mapped_column(nullable=True)
  # GC_Free = Column(mssql.MONEY, nullable=True)
  # ScaleItemType = Column(mssql.INTEGER, nullable=True)
  # ObjectID = Column(mssql.NVARCHAR(length=40), nullable=True)
  # ParentObjectID = Column(mssql.NVARCHAR(length=40), nullable=True)
  BulkRate = Column(mssql.NVARCHAR(length=25), nullable=True)
  # SecurityDeposit = Column(mssql.MONEY, nullable=True)
  # Liability = Column(mssql.MONEY, nullable=True)
  SalePricePer: Mapped[Decimal] = mapped_column(nullable=True)
  # Line_Tax_Exempt_2 = Column(mssql.BIT, nullable=True)
  # Line_Tax_Exempt_3 = Column(mssql.BIT, nullable=True)
  # modifierPriceLock = Column(mssql.BIT, nullable=False)
  # Salesperson = Column(mssql.NVARCHAR(length=10), nullable=True)
  # ComboApplied = Column(mssql.BIT, nullable=True)
  # KitchenQuantityPrinted: Mapped[Decimal] = mapped_column(nullable=True)
  PricePerBeforeDiscount: Mapped[Decimal] = mapped_column(nullable=False)
  # OrigPriceSetBy = Column(mssql.INTEGER, nullable=True)
  PriceChangedBy = Column(mssql.INTEGER, nullable=True)
  # Kit_Override = Column(mssql.MONEY, nullable=True)
  # KitTotal = Column(mssql.MONEY, nullable=True)
  # SentToKitchen = Column(mssql.BIT, nullable=True)
  # OnlineLoyalty_OfferID = Column(mssql.NVARCHAR(length=25), nullable=True)
  # Tax4Per: Mapped[Decimal] = mapped_column(nullable=True)
  # Tax5Per: Mapped[Decimal] = mapped_column(nullable=True)
  # Tax6Per: Mapped[Decimal] = mapped_column(nullable=True)
  # Line_Tax_Exempt_4 = Column(mssql.BIT, nullable=True)
  # Line_Tax_Exempt_5 = Column(mssql.BIT, nullable=True)
  # Line_Tax_Exempt_6 = Column(mssql.BIT, nullable=True)
  # InsertOriginatorId = Column(mssql.INTEGER, nullable=True)
  # UpdateOriginatorId = Column(mssql.INTEGER, nullable=True)
  # UpdateTimestamp = Column(mssql.TIMESTAMP, nullable=False)
  # ModifiedDate = Column(mssql.DATETIME, nullable=True)
  # CreateDate = Column(mssql.DATETIME, nullable=True)
  # CreateTimestamp = Column(mssql.BIGINT, nullable=True)


class InventoryBulkInfo(Base):
  __tablename__ = "Inventory_Bulk_Info"
  ItemNum = Column(
    mssql.NVARCHAR(length=20), ForeignKey(Inventory.ItemNum), primary_key=True, nullable=False
  )
  Store_ID = Column(
    mssql.NVARCHAR(length=10), ForeignKey(Inventory.Store_ID), primary_key=True, nullable=False
  )
  Bulk_Price: Mapped[Decimal] = mapped_column(nullable=False)
  Bulk_Quan: Mapped[Decimal] = mapped_column(primary_key=True, nullable=False)
  ...
