

#Vendor 360 Dashboard 
#Version - 2.0

#Loading required packages
library(readxl)
library(writexl)
library(sqldf)
library(purrr)
library(dplyr)
library(zoo)
library(data.table)


#Importing the data
uc_raw <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/UC_Sales.xlsx",guess_max = 1048576)
uc_item_master <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/UC_Item_Master.xlsx")
calendar <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/Calendar.xlsx")
Returns <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/Returns.xlsx")
DH_Data <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/DH_Charges_Data.xlsx")
SR_Data <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/SR_Charges_Data.xlsx")
PK_Data <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/Picker_Charges_Data.xlsx")
Pincode_Data <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/Mod Pincode Data.xlsx")
SubCatMap <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/SubCatMap.xlsx")
Shopify <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/Shopify_Product_Export_2023_07.xlsx")
uc_inventory <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/UC_Invt_2022_11_15.xlsx")
uc_item_master_2 <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/UC_Item_Master_New.xlsx")
Matrixify <- read_excel("C:/Users/harsh/Desktop/Vaaree/Sales/Matrixify.xlsx",sheet = "Products")

#Retaining only the required columns & renaming 
old_column_names <- c("Sale Order Item Code", "Display Order Code", "Reverse Pickup Code", "COD", "Category", "Shipping Address Pincode", "Item SKU Code", "Channel Product Id", "MRP", "Selling Price", "Cost Price", "Voucher Code", "Packet Number", "Order Date as dd/mm/yyyy hh:MM:ss", "Sale Order Status", "Sale Order Item Status", "Cancellation Reason", "Shipping provider", "Shipping Courier", "Shipping Package Creation Date", "Shipping Package Status Code","Delivery Time","Tracking Number", "Dispatch Date", "Facility", "Return Reason","Seller SKU Code","Billing Address Phone","Discount","CGST","IGST","SGST","Shipping Charges","COD Service Charges","Item Type EAN")
new_column_names <- c("SaleOrderItemCode","DisplayOrderCode","ReversePickupCode","COD","Category","ShippingAddressPincode","ItemSKUCode","ChannelProductId","MRP","SellingPrice","CostPrice","VoucherCode","UnitsSold","OrderDate","SaleOrderStatus","SaleOrderItemStatus","CancellationReason","ShippingProvider","ShippingCourier","ShippingPackageCreationDate","ShippingPackageStatusCode","DeliveryTime","AWB","DispatchDate","Facility","ReturnReason","SellerSKU","BillingAddressPhone","Discount","CGST","IGST","SGST","ShippingCharges","CODServiceCharges","ListingPrice")

uc_raw <- uc_raw[, old_column_names]
colnames(uc_raw) <- new_column_names

#Modifying the columns in the required format
uc_raw$OrderDate <- as.character(as.Date(uc_raw$OrderDate))
uc_raw$ShippingPackageCreationDate <- as.character(as.Date(uc_raw$ShippingPackageCreationDate))
uc_raw$DeliveryTime <- as.character(as.Date(uc_raw$DeliveryTime))
uc_raw$DispatchDate <- as.character(as.Date(uc_raw$DispatchDate))
uc_raw$DaysToDispatch <- as.numeric(as.Date(uc_raw$DispatchDate) - as.Date(uc_raw$OrderDate))
uc_raw$DaysToDeliver <- as.numeric(as.Date(uc_raw$DeliveryTime) - as.Date(uc_raw$OrderDate))
uc_raw$BillingAddressPhone <- gsub(' ', '', uc_raw$BillingAddressPhone,fixed = TRUE)
uc_raw$OrderMonth <- format.Date(uc_raw$`OrderDate`, '%Y-%m')
Today <- as.Date(Sys.Date())
uc_item_master$Created <- as.character(as.Date(uc_item_master$Created))
Matrixify$`Created At` <- substring(Matrixify$`Created At`,1,7)

#Fixing the level of this table
Shopify$`Variant SKU` <- gsub("'", '', Shopify$`Variant SKU`)
Shopify$Type[Shopify$Type == ""] <- NA
Shopify$Type <- na.locf(Shopify$Type)


Shopify_Mod <- sqldf("select `Variant SKU` as SKU,
                            max(Type) as SubCat,
                            max(Vendor) as Vendor
                    from Shopify
                    where length(`Variant SKU`) >0
                    group by 1")

Returns_Mod <- sqldf("select `Order Number`,
                             `SKU`,
                              sum(`QNT`) as TOT_QNT,
                              max(case when `VV Reason` = 'Customer Reasons' then 1 else 0 end) as cust_reason_flag,
                              max(case when `VV Reason` = 'Defective/Damaged' then 1 else 0 end) as defective_flag,
                              max(case when `VV Reason` = 'Wrong Dispatch' then 1 else 0 end) as wrong_dispatch_flag
                      from Returns
                      where `Order Number` is not null and `SKU` is not null
                      group by 1,2")


uc_raw <- sqldf("select t1.*,
                        case when ReversePickupCode is not null then -1
                             when SaleOrderStatus = 'COMPLETE' 
                              and SaleOrderItemStatus in ('DISPATCHED','DELIVERED')
                              and ShippingPackageStatusCode in ('DISPATCHED','DELIVERED')
                              and ReversePickupCode is null
                              then 1 else 0
                        end as OrderStatusMod
                 from uc_raw as t1")

uc_raw <- uc_raw[order(uc_raw$DisplayOrderCode, uc_raw$SellerSKU, uc_raw$OrderStatusMod), ]

setDT(uc_raw)  # Convert uc_raw to a data.table for faster operations

uc_raw[, row_number := 1:.N, by = .(DisplayOrderCode, SellerSKU)]


uc_raw<- sqldf("select t1.*,
                       case when t2.TOT_QNT is not null
                             and t1.row_number <= t2.TOT_QNT
                            then -1 else OrderStatusMod end as OrderStatusModdd,
                       case when t2.defective_flag = 1 then 'Defective/Damaged'
                            when t2.wrong_dispatch_flag = 1 then 'Wrong Dispatch'
                            when t2.cust_reason_flag = 1 then 'Customer Reasons' end as ReturnReasonnn
                from uc_raw as t1
                left join Returns_Mod as t2
                       on t1.DisplayOrderCode = t2.`Order Number`
                      and t2.SKU = t1.SellerSKU")

uc_mod <- sqldf("select t1.SaleOrderItemCode,
            				    t1.DisplayOrderCode as OrderNo,
            				    case when t1.COD=0 then 'Prepaid' else 'COD' end as CODFlag,
						            t1.AWB,					
                        t1.OrderDate,
						            t1.DispatchDate,
                        t1.DeliveryTime as DeliveryDate,
                        t1.DaysToDispatch,
                        t1.DaysToDeliver,
                        t3.wk_nm as Week,
                        t3.mnt_no as Month,							
                        t1.Facility,
						            t1.ShippingProvider,
                        case when (t1.SaleOrderStatus = 'CANCELLED' or t1.SaleOrderItemStatus = 'CANCELLED') then 'Cancel'
                             when t1.SaleOrderStatus = 'PROCESSING' then 'Processing'
							               when t1.SaleOrderStatus = 'COMPLETE' and t1.SaleOrderItemStatus in ('DISPATCHED','DELIVERED') and (t1.ReversePickupCode is not null or t1.OrderStatusModdd = -1) then 'Return'
                             when t1.SaleOrderStatus = 'COMPLETE' and t1.SaleOrderItemStatus in ('DISPATCHED','DELIVERED')
                              and t1.ShippingPackageStatusCode in ('RETURN_EXPECTED','RETURNED') then 'RTO'
							               when t1.ShippingPackageStatusCode in ('SHIPPED') then 'In Transit'
                             when t1.SaleOrderStatus = 'COMPLETE'
                              and t1.SaleOrderItemStatus in ('DISPATCHED','DELIVERED') 
                              and t1.ShippingPackageStatusCode in ('DISPATCHED','DELIVERED') then 'COMPLETE'
                        end as OrderStatus,
                        t1.ReturnReasonnn as ReturnReason,
                        t1.ItemSKUCode,
                        t1.ChannelProductID,
                        t1.SellerSKU,
                        t2.`Product Name on Channel` as ProductName,
                        t1.Category as UC_Category,	
            						t5.SubCat,
            						t7.Category,
            						t7.Dep as Department,
            						t1.VoucherCode,
            						t1.ShippingAddressPincode as Pincode,
                        t4.State,
                        t4.District,
            						t1.BillingAddressPhone,	
            						t1.OrderMonth||'-01' as OrderMonthDate,
            						t6.FirstOrderMonth||'-01' as FirstOrderDate,
            						t6.FirstOrderMonth,
            						case when t8.SellingPrice <1000 then '<1K'
                          when t8.SellingPrice <2000 then '1K to 2K'
                          when t8.SellingPrice <3000 then '2K to 3K'
                          when t8.SellingPrice <4000 then '3K to 4K'
                          when t8.SellingPrice >=4000 then '4K+'
                        end as AOV_Buckets,
                        case when t8.UnitsSold =1 then '1'
                          when t8.UnitsSold =2 then '2'
                          when t8.UnitsSold <=5 then '3 to 5'
                          when t8.UnitsSold >5 then '5+'
                        end as AOQ_Buckets,
                        sum(t1.MRP) as MRP,
                        sum(t1.SellingPrice) as SellingPrice,
                        sum(t1.ListingPrice) as ListingPrice,
                        sum(t1.CostPrice) as CostPrice,
                        sum(t1.UnitsSold) as UnitsSold,
                        sum(t1.Discount) as Discount,
                        sum(coalesce(t1.CGST,0) + coalesce(t1.SGST,0) + coalesce(t1.IGST,0)) as Tax,
                        sum(coalesce(t1.ShippingCharges,0)+coalesce(t1.CODServiceCharges,0)) as CODCharges
                                              

                 from uc_raw as t1
                 left join uc_item_master as t2
                        on t1.ChannelProductId = t2.`Channel Product Id`
                 left join calendar as t3
                        on t1.OrderDate = t3.dt
                 left join Pincode_Data as t4
                        on t1.ShippingAddressPincode = t4.Pincode
                 left join Shopify_Mod as t5
                        on t5.SKU = t1.SellerSKU
						     left join (select BillingAddressPhone,min(OrderMonth) as FirstOrderMonth
						                from uc_raw
						                where SaleOrderStatus = 'COMPLETE'
                              and SaleOrderItemStatus in ('DISPATCHED','DELIVERED') 
                              and ShippingPackageStatusCode in ('DISPATCHED','DELIVERED')
                            group by 1) as t6
                        on t1.BillingAddressPhone = t6.BillingAddressPhone
                 left join SubCatMap as t7
                        on t5.SubCat = t7.SubCat
                 left join (select DisplayOrderCode,
                                   sum(SellingPrice) as SellingPrice,
                                   count(*) as UnitsSold
						                from uc_raw
						                where SaleOrderStatus = 'COMPLETE'
                              and SaleOrderItemStatus in ('DISPATCHED','DELIVERED') 
                              and ShippingPackageStatusCode in ('DISPATCHED','DELIVERED')
                            group by 1) as t8
                        on t1.DisplayOrderCode = t8.DisplayOrderCode
                 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32")


uc_mod$OrderMonthDate <- as.Date(uc_mod$OrderMonthDate)
uc_mod$FirstOrderDate <- as.Date(uc_mod$FirstOrderDate)


elapsed_months <- function(end_date, start_date) {
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}


uc_mod$RunningMonth <- elapsed_months(uc_mod$OrderMonthDate,uc_mod$FirstOrderDate) 
uc_mod$BillingAddressPhone <- as.character(uc_mod$BillingAddressPhone)


#Inventory Module
max_date <- as.Date(sqldf("select max(OrderDate) from uc_mod")[1,1])
min_date <- max_date - 31

ros <- sqldf(paste0("select ItemSKUCode,
                  					Facility,
                  					UC_Category,
                  					SubCat,
                  					Category,
                  					Department,
                  					ProductName,
                  					sum(UnitsSold)/30 as ROS,
                  					avg(SellingPrice/UnitsSold) * sum(UnitsSold) as RevLoss
                  					
                     from uc_mod 
                  	 where OrderDate between '",min_date,"' and '",max_date,"' 
                  	 group by 1,2,3,4,5,6,7"))

min_date <- NULL
max_date <- NULL

uc_inventory <- sqldf("select `Item SkuCode` as ItemSKUCode,
                               sum(Inventory) as SOH
                       from uc_inventory
                       group by 1")

ros <- sqldf("select t1.*,
                     t2.SOH
              from ros as t1
              left join uc_inventory as t2
                     on t1.ItemSKUCode = t2.ItemSKUCode")

#Item Module
uc_cat <- sqldf("select t1.ChannelProductId,
                        t1.Category,
                        t2.mnt_no as Order_Month,
                        sum(t1.`SellingPrice`) as Revenue
                        
                 from uc_mod as t1
                 left join calendar as t2
                        on t1.OrderDate = t2.dt
                 where t1.OrderStatus = 'COMPLETE'
                 group by 1,2,3")


cat_1 <- sqldf("select t1.`SKU Code`,
                       t5.Vendor as Brand,
                       t2.`Category Name` as Category,
                       t5.`Type` as SubCategory,
                       t3.mnt_no as Create_Month,
                       case when t1.`Last Inventory Update` >0 then 'InStock' else 'OOS' end as Instock_Flag,
                       coalesce(t4.Revenue,0) as Revenue
                from uc_item_master as t1
                left join uc_item_master_2 as t2
                       on t1.`SKU Code` = t2.`Product Code`
                left join calendar as t3
                       on t1.Created = t3.dt
                left join (select ChannelProductId,sum(Revenue) as Revenue
                           from uc_cat
                           group by 1) as t4
                       on t4.ChannelProductId = t1.`Channel Product Id`
                left join Shopify as t5
                       on t1.`Seller SKU on Channel` = t5.`Variant SKU`
                where t1.ListingStatus like 'ACTIVE'")


cat_2 <- sqldf("select Order_Month,
                       Category,
                       t5.`Type` as SubCategory,
                       t5.Vendor as Brand,
                       t3.mnt_no as Launch_Month,
                       sum(Revenue) as Revenue
    
                from uc_cat as t1
                join uc_item_master as t2
                      on t1.ChannelProductId = t2.`Channel Product Id`
                left join calendar as t3
                      on t2.Created = t3.dt
                left join uc_item_master_2 as t4
                      on t2.`SKU Code` = t4.`Product Code`
                left join Shopify as t5
                      on t2.`Seller SKU on Channel` = t5.`Variant SKU`
                where t2.ListingStatus like 'ACTIVE' 
                group by 1,2,3,4,5")

#Shipping Charge Module
DH_Data$pickup_date <- as.character(as.Date(DH_Data$pickup_date))
DH_Data <- sqldf("select waybill_num as AWB,
                         order_id as OrderNo,
                         pickup_date as OrderDate,
                         sum(charge_cod) as CODCost,
                         sum(total_amount) as ShippingCharge
                  from DH_Data
                  group by 1,2,3
                  having ShippingCharge > 0")

DH_Data$AWB <- as.character(DH_Data$AWB) 

SR_Data$`Date, Time` <- as.character(as.Date(SR_Data$`Date, Time`))
SR_Data <- sqldf("select `AWB Number` as AWB,
                         `Order Number` as OrderNo,
                         `Date, Time` as OrderDate,
                         sum(coalesce(`Cod Charges`,0)) as CodCost,
                         sum(`Freight Total Amount`) as ShippingCharge
                  from SR_Data
                  group by 1,2,3
                  having ShippingCharge > 0")

PK_Data$`Pickup Date` <- as.character(as.Date(PK_Data$`Pickup Date`))
PK_Data <- sqldf("select `Tracking ID` as AWB,
                         `Client Order ID` as OrderNo,
                         `Pickup Date` as OrderDate,
                         SUM(`COD Charges`) as CodCost,
                         sum(`Freight Charges`) as ShippingCharge
                  from PK_Data
                  group by 1,2,3
                  having ShippingCharge > 0")


ship_chrg <- sqldf("select * from DH_Data 
                    union 
                    select * from SR_Data
                    union 
                    select * from PK_Data")

uc_mod_cntr <- sqldf("select AWB,
                             Facility,
                             OrderDate,
                             ShippingProvider,
                             Pincode,
                             State,
                             District,
                             sum(SellingPrice) as SellingPrice
                      from uc_mod
                      group by 1,2,3,4,5,6,7")


ship_chrg <- sqldf("select t1.AWB,
                           t1.OrderNo,
                           t2.Facility,
                           coalesce(t2.OrderDate,t1.OrderDate) as OrderDate,
                           t2.ShippingProvider,
                           t2.Pincode,
                           t2.State,
                           t2.District,
                           case when t1.OrderNo like '%-RP%' then 'Reverse Pickup'
                                when t2.Facility is null then 'Internal'
                                else 'Regular'
                           end as ShipChargeType,
                           t1.CODCost,
                           t1.ShippingCharge,
                           t2.SellingPrice
                    from ship_chrg as t1
                    left join uc_mod_cntr as t2
                           on t1.AWB = t2.AWB")


facility_map <- sqldf("select Facility
                       from uc_mod_cntr
                       where Facility is not null
                       group by 1")



#PnL Module
PnL <- sqldf("select Month,
                     Facility,
                     OrderNo, 
                     CODFlag,
                     AWB,
                     max(case when OrderStatus = 'COMPLETE' then 0
                              else 1
                           end) as OrderStatus,
                     sum(Tax) as Tax,
                     sum(case when OrderStatus = 'COMPLETE' then CostPrice else 0 end) as CostPrice,
                     sum(case when OrderStatus = 'COMPLETE' then ListingPrice else 0 end) as LP,
                     sum(case when OrderStatus = 'COMPLETE' then Discount else 0 end) as Discount,
                     sum(case when OrderStatus = 'COMPLETE' then CODCharges else 0 end) as CODCharges,
                     sum(case when UC_Category in ('CUSHIONS','TOWEL','BATH') then 5
                              when UC_Category in ('BEDSHEET','THROWS','DIWAN Set') then 10
                              when Category in ('KITCHEN','DECOR') then 2 end) as PackagingCost
              from uc_mod
              where Month in ('2023-06','2023-05','2023-04','2023-03','2023-02','2023-01','2022-12','2022-11','2022-10','2022-09')
              group by 1,2,3,4,5")

PnL <- sqldf("select t1.Month,
                     t1.Facility,
                     t1.OrderNo,
                     t1.CODFlag,
                     t1.AWB,
                     t1.Tax,
                     t1.OrderStatus,
                     t1.CostPrice,
                     t1.LP,
                     t1.Discount,
                     t1.PackagingCost,
                     t1.CODCharges,
                     case when (t2.CODCost is null and t1.CODFlag = 'COD') then (t1.LP - t1.Discount + t1.CODCharges)*0.015/(1+(t1.Tax)/100) 
                          when (t2.CODCost is null and t1.CODFlag = 'Prepaid') then 0
                          else t2.CODCost end as CODCost,
                     coalesce(t2.ShippingCharge,125) as ShippingCost
              from PnL as t1
              left join ship_chrg as t2
                     on t1.AWB = t2.AWB")

PnL <- sqldf("select Month,
                     Facility,
                     coalesce(sum(CostPrice),0) as CostPrice,
                     coalesce(sum(LP),0) as ListingPrice,
                     coalesce(sum(Discount),0) as Discount,
                     coalesce(sum(CODCharges),0) as CODCharges,
                     coalesce(sum(case when OrderStatus = 0 then (LP - Discount + CODCharges)-((LP - Discount - CODCharges)/(1+ Tax/100))  else 0 end),0) as Tax1,
                     coalesce(sum(PackagingCost),0) as PackagingCost,
                     coalesce(sum(CODCost),0) as CODCost,
                     coalesce(sum(ShippingCost),0) as ShippingCost,
                     coalesce(sum(CODCost + ShippingCost) * 0.18,0) as Tax2 
              from PnL
              group by 1,2")

#Sourcing Module
Calendar_Mod <- sqldf("select mnt_no from calendar group by 1")

Matrixify_Mod <- sqldf("select  ID,
                               `Metafield: my_fields.material [list.single_line_text_field]` as Material,
                               `Metafield: custom.thread_count [number_integer]` as TC
                        from Matrixify
                        where Material is not null or TC is not null
                        group by 1,2,3")

Shopify_Mod <- sqldf("select Handle,Vendor,Type
                      from Shopify
                      where length(Vendor) >0 or length(Type) >0
                      group by 1,2,3")


Matrixify_Mod$Material <- gsub('[', '', Matrixify_Mod$Material,fixed = TRUE)
Matrixify_Mod$Material <- gsub(']', '', Matrixify_Mod$Material,fixed = TRUE)
Matrixify_Mod$Material <- gsub('"', '', Matrixify_Mod$Material,fixed = TRUE)


Bedsheets <- sqldf("select t1.`Variant SKU` as VariantSKU,
                           t4.Vendor,
                           t4.Type as SubCategory,
                           t1.`Option1 Value` as Size,
                           t2.`Created At` as CreateMonth,
                           t3.Material,
                           t3.TC,
                           t1.`Variant Price` as SP,
                           t1.`Cost per item` as CP
                    from Shopify as t1
                    left join Matrixify as t2 
                           on t1.`Variant SKU` = t2.`Variant SKU`
                    left join Matrixify_Mod as t3
                           on t2.ID = t3.ID
                    left join Shopify_Mod as t4
                           on t1.Handle = t4.Handle
                    where t4.Type = 'Bedsheets'
                    group by 1,2,3,4,5,6,7,8,9")

Dohar <- sqldf("select t1.`Variant SKU` as VariantSKU,
                       t1.Vendor,
                       t1.Type as SubCategory,
                       case when t1.`Option1 Value` like '%GSM%' then 'King' else t1.`Option1 Value` end as Size,
                       t2.`Created At` as CreateMonth,
                       t3.Material,
                       NULL as TC,
                       t1.`Variant Price` as SP,
                       t1.`Cost per item` as CP
                from Shopify as t1
                left join Matrixify as t2 
                       on t1.`Variant SKU` = t2.`Variant SKU`
                left join Matrixify_Mod as t3
                       on t2.ID = t3.ID
                where t1.Type in ('Dohars','Comforters','Curtains')
                group by 1,2,3,4,5,6,7,8,9")


Cushions <- sqldf("select t1.`Variant SKU` as VariantSKU,
                          t1.Vendor,
                          t1.Type as SubCategory,
                          t1.`Option1 Value` as Size,
                          t2.`Created At` as CreateMonth,
                          t3.Material,
                          NULL as TC,
                          t1.`Variant Price` as SP,
                          t1.`Cost per item` as CP
                   from Shopify as t1
                   left join Matrixify as t2 
                          on t1.`Variant SKU` = t2.`Variant SKU`
                   left join Matrixify_Mod as t3
                          on t2.ID = t3.ID
                   where t1.Type like '%Cushion Cover%'
                   group by 1,2,3,4,5,6,7,8,9")

Lamps <- sqldf("select t1.`Variant SKU` as VariantSKU,
                       t1.Vendor,
                       t1.Type as SubCategory,
                       NULL as Size,
                       t2.`Created At` as CreateMonth,
                       t3.Material,
                       NULL as TC,
                       t1.`Variant Price` as SP,
                       t1.`Cost per item` as CP
                 from Shopify as t1
                 left join Matrixify as t2 
                        on t1.`Variant SKU` = t2.`Variant SKU`
                 left join Matrixify_Mod as t3
                        on t2.ID = t3.ID
                 where upper(t1.Type) like '%LAMP%'
                 group by 1,2,3,4,5,6,7,8,9")

Clocks <- sqldf("select t1.`Variant SKU` as VariantSKU,
                        t1.Vendor,
                        t1.Type as SubCategory,
                        NULL as Size,
                        t2.`Created At` as CreateMonth,
                        t3.Material,
                        NULL as TC,
                        t1.`Variant Price` as SP,
                        t1.`Cost per item` as CP
                 from Shopify as t1
                 left join Matrixify as t2 
                        on t1.`Variant SKU` = t2.`Variant SKU`
                 left join Matrixify_Mod as t3
                        on t2.ID = t3.ID
                 where upper(t1.Type) like '%CLOCK%' 
                 group by 1,2,3,4,5,6,7,8,9")

Wall <- sqldf("select t1.`Variant SKU` as VariantSKU,
                      t1.Vendor,
                      t1.Type as SubCategory,
                      NULL as Size,
                      t2.`Created At` as CreateMonth,
                      t3.Material,
                      NULL as TC,
                      t1.`Variant Price` as SP,
                      t1.`Cost per item` as CP
               from Shopify as t1
               left join Matrixify as t2 
                      on t1.`Variant SKU` = t2.`Variant SKU`
               left join Matrixify_Mod as t3
                      on t2.ID = t3.ID
               where upper(t1.Type) not like '%LAMP%'
                 and upper(t1.Type) not like '%CLOCK%'
                 and upper(t1.Type) not like '%HOOK%'
                 and upper(t1.Type) not like '%SHELF%'
                 and upper(t1.Type) not like '%CUSHION COVER%'
                 and t1.Type not in ('Dohars','Comforters','Curtains','Bedsheets')
               group by 1,2,3,4,5,6,7,8,9")

Textiles <- sqldf("select * from Bedsheets union
                   select * from Cushions union
                   select * from Dohar union
                   select * from Lamps union
                   select * from Clocks union
                   select * from Wall")


Sales_DF <- sqldf("select Month as ReportMonth,
                          SellerSKU,
                          sum(UnitsSold) as UnitsSold,
                          sum(ListingPrice) as ListingPrice
                   from uc_mod
                   where OrderStatus = 'COMPLETE'
                   group by 1,2")

sourcing <- sqldf("select t2.mnt_no as ReportMonth,
                          SubCategory,
                          Vendor,
                          Size,
                          Material,
                          TC,
                          count(distinct t1.VariantSKU) as SKUCount,
                          avg(t1.SP) as AvgSP,
                          avg(t1.CP) as AvgCP,
                          coalesce(sum(t3.UnitsSold),0) as UnitsSold,
                          coalesce(sum(t3.ListingPrice),0) as ListingPrice,
                          t4.Dep
                   from Textiles as t1
                   left join Calendar_Mod as t2
                          on t1.CreateMonth <= t2.mnt_no
                   left join Sales_DF as t3
                          on t1.VariantSKU = t3.SellerSKU
                         and t2.mnt_no = t3.ReportMonth
                   left join SubCatMap as t4
                          on t1.SubCategory = t4.SubCat
                   where t2.mnt_no <= '2023-06' and t2.mnt_no is not null and Dep is not null
                   group by 1,2,3,4,5,6,12")


#Dumping all tables that are not required

uc_raw <- NULL
DH_Data <- NULL
SR_Data <- NULL
Pincode_Data <- NULL
Returns <- NULL
Returns_Mod <- NULL
Shopify <- NULL
Shopify_Mod <- NULL
SubCatMap <- NULL
uc_item_master <- NULL
uc_inventory <- NULL
PK_Data <- NULL
uc_item_master_2 <- NULL
DH_Data <- NULL
SR_Data <- NULL
PK_Data <- NULL
uc_mod_cntr <- NULL
uc_cat <- NULL

Bedsheets <- NULL
Calendar_Mod <- NULL
Clocks <- NULL
Cushions <- NULL
Dohar <- NULL
Lamps <- NULL
Wall <- NULL
Matrixify <- NULL
Matrixify_Mod <- NULL
Textiles <- NULL
Sales_DF <- NULL


write_xlsx(uc_mod,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/uc_mod.xlsx")
write_xlsx(ros,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/ros.xlsx")
write_xlsx(PnL,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/PnL.xlsx")
write_xlsx(ship_chrg,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/ship_chrg.xlsx")
write_xlsx(sourcing,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/sourcing.xlsx")
write_xlsx(cat_1,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/cat1.xlsx")
write_xlsx(cat_2,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/cat2.xlsx")
write_xlsx(calendar,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/calendar.xlsx")
write_xlsx(facility_map,"C:/Users/harsh/Desktop/Vaaree/Dashboard/Dashboards 2.0/facility_map.xlsx")

#write.table(uc_mod, "clipboard-131072",na="", sep="\t", col.names=TRUE,row.names = FALSE)

