USE [PROJECT]
GO

/****** Object:  StoredProcedure [dbo].[MarketAbuse_Daily_Insert_InsiderDealing_TradingAheadOfNewsEvent_Cancelled_PO]    Script Date: 11/11/2021 15:52:21 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








CREATE procedure [dbo].[MarketAbuse_Daily_Insert_InsiderDealing_TradingAheadOfNewsEvent_Cancelled_PO] @td date
------ 2020-09-04 RM - added baskets to logic
------ 2020-10-08 RM - changed logic to exclude open trades which were closed out before time of event
------ 2020-10-13 RM - added notional vol, price move, and pnl thresholds
------ 2020-10-20 RM - added entity columns
------ 2020-11-10 RM - removed open trades which have been closed before news event
------ 2020-04-21 RM - added cancelled pending orders
as


--declare @td date = '2021-04-23'
SET DATEFIRST 1

declare  @today varchar(10), @rundt datetime, @run_dt varchar(11) , @reportdt_start datetime , @reportdt_end datetime, @monthyyyy as varchar (15)
		, @mon as varchar(2), @year as varchar(4), @yyyymm_mon as varchar(10), @yyyymm as varchar(6),@yyyymmddt varchar(30), @usd_yyyymm_mon as varchar(10), @cp_desc_open as varchar (max),@cp_desc_close as varchar(max)
		, @cp_asc_close as varchar (max), @cp_asc_open as varchar (max), @hoursfromeod as int, @hoursfromsod as int

set @hoursfromeod = ( select  24 - (16 + cast(UTCOffset as int)) from keys..calendar where cast(dt as date) = cast(getdate() as date) ) -- hours to subtract from 24:00:00 to get us closingtimeutc
set @hoursfromsod = ( select  16 + cast(UTCOffset as int) from keys..calendar where cast(dt as date) = cast(getdate() as date) ) -- hours to subtract add 24:00:00 to get us closingtimeutc
if @td is null --if no date supplied then run date is day before today
	begin 
		set @td = getdate()
		set @today  = datepart(weekday,@td) %7

		if @today = 1 -- if monday then run date is friday
			begin
				set @rundt  = cast(dateadd(dd,-3,@td)as date)
				set @reportdt_start  =  dateadd(hh,-@hoursfromeod,@rundt) -- alert timeframe is [ d-1 16:00  --> d 16:00 ]
				set @reportdt_end  =  dateadd(hh,@hoursfromsod,@rundt)

			end

		if @today = 2 -- if tuesday then run date is monday and alert timeframe is fri 16:00 --> mon 16:00
			begin
				set @rundt  = cast(dateadd(dd,-1,@td)as date)
				set @reportdt_start  =  dateadd(dd,-2,dateadd(hh,-@hoursfromeod,@rundt))
				set @reportdt_end  =  dateadd(hh,@hoursfromsod,@rundt)
			end
		if @today >2
			begin
				set @rundt  = cast(dateadd(dd,-1,@td)as date)
				set @reportdt_start  = dateadd(hh,-@hoursfromeod,@rundt)
				set @reportdt_end  =  dateadd(hh,@hoursfromsod,@rundt)
			end
	end
else --if date then run date is date
	begin
		set @td = @td 
		set @today  = datepart(weekday,@td) %7

		if @today = 1
			begin
				set @rundt  = @td 
				set @reportdt_start  =  dateadd(dd,-3,dateadd(hh,-@hoursfromeod,@rundt))
				set @reportdt_end  =  dateadd(hh,@hoursfromsod,@rundt)
			end
		if @today >=2
			begin
				set @rundt  = @td
				set @reportdt_start  = dateadd(hh,-@hoursfromeod,@rundt)
				set @reportdt_end  =  dateadd(hh,@hoursfromsod,@rundt)
			end
	end


--select @rundt[rundt], @reportdt_start [start], @reportdt_end [end]


-- determine hdm name

-- Set Monday to be weekday 1 (default is Sunday)
set datefirst 1;
	
-- Get the date of last Sunday
Declare @CurrentDay as int  =  DATEPART(weekday, @rundt )
Declare @LastSundayDate as datetime = DATEADD( day,  -1 * ( @CurrentDay % 7) , @rundt )

 -- In case today is January 1st. The year parameter must be equal to the previous year (XXXX -1). 
Declare @Year_hdm as int = case when month(@rundt) = 1  then YEAR(dateadd(month,-1,@rundt)) 
						else YEAR(@rundt) end
-- Current month
Declare @Month as varchar(2) = right('0'+ cast( datepart(month, @LastSundayDate)  as varchar) ,2 )
-- Curent Day
Declare @Day as varchar(2) = right('0'+ cast( datepart(day, @LastSundayDate)  as varchar) ,2 )
-- Previous month
Declare @P_Month as varchar(2) = right('0'+ cast( datepart(month, dateadd(month,-1, @rundt))  as varchar) ,2 )
-- Exctract Previous month name
Declare @P_Month_Name as varchar(3) = datename(month,dateadd(month,-1, @rundt))
--Create the table name: 
	--logic: If the last Sunday was in the previous month or last Sunday is the first day of the current month: 
			-- then  the monthly HD table is to be created and the keys..hdm_yyyymm_mon format is to be used
			-- else	 the weekly	 HD table is to be created	and the keys..hdm_yyyymm_mon_mmdd format is to be used		
Declare @hdm_file_name as varchar(50) = 
	case when right( '0'+  cast( datepart(month,@LastSundayDate ) as varchar(2)) ,2)  =  @P_Month 
				or  right( '0'+  cast( datepart(day,@LastSundayDate ) as varchar(2)) ,2) like '01'
			then 'keys..hdm_' + cast(@Year_hdm as varchar(4) ) + @P_Month + '_' + @P_Month_Name 
	 else 
			'keys..hdm_' + cast(@Year_hdm as varchar(4))  + @P_Month + '_'+ @P_Month_Name + '_' +  @Month + @Day
	end
	
--select @hdm_file_name


if object_id('tempdb..#octimes') is not null
					drop table #octimes;
					drop table #octimes;
;
with octimes as 
(
--get opening and closing times of stock and convert to utc and utc conversion hours
select instrument, null [basket]
, case when openingtimezone like 'BST' then dateadd(hour, -c.GMTtoUTC, openingtime)
		when openingtimezone like 'CET' then dateadd (hour, -c.CETtoUTC, openingtime)
		when openingtimezone like 'EST' then dateadd(hour, c.UTCoffset, openingtime)
		when openingtimezone like 'HKT' then dateadd(hour, -c.HKTtoUTC, openingtime)
		end openingtimeutc
, case when openingtimezone like 'BST' then dateadd(hour, -c.GMTtoUTC, closingtime)
		when openingtimezone like 'CET' then dateadd (hour,-c.CETtoUTC, closingtime)
		when openingtimezone like 'EST' then dateadd(hour, c.UTCoffset, closingtime)
		when openingtimezone like 'HKT' then dateadd(hour, -c.HKTtoUTC, closingtime)
		end closingtimeutc
,c.GMTtoUTC,c.CETtoUTC,c.UTCOffset
from keys..sharecfd s
left join keys..calendar c on c.dt=@rundt

union all

select s.item [instrument],b.instrument [basket]
		, case when openingclosingtimezone in ('ET','EST')
					then isnull(dateadd(hour, c.UTCoffset, openingtime),dateadd(hour, -c.GMTtoUTC,openingtimeGMT))
				when openingclosingtimezone like 'HKT'
					then isnull(dateadd(hour, -c.HKTtoUTC, openingtime),dateadd(hour, -c.GMTtoUTC,openingtimeGMT))
				else openingtimeGMT end openingtimeutc
		, case when openingclosingtimezone in ('ET','EST')
					then coalesce(dateadd(hour, c.UTCoffset, closingtime),dateadd(hour, -c.GMTtoUTC,closingtimeGMT))
				when openingclosingtimezone like 'HKT'
					then coalesce(dateadd(hour, -c.HKTtoUTC, closingtime),dateadd(hour, -c.GMTtoUTC,closingtimeGMT))
				else closingtimeGMT end closingtimeutc
		,c.GMTtoUTC,c.CETtoUTC,c.UTCOffset
from keys..basket b
	left join keys..calendar c on c.dt=@rundt
	cross apply keys.dbo.fSplitStrings(b.[UnderlyingStocks], ';') as s
where b.instrumentassetgroup like 'Stock Basket'
)

select *
into #octimes
from octimes

if object_id('tempdb..#MarketAbuseEquityNews_mindt') is not null
	begin
		drop table #MarketAbuseEquityNews_mindt
	end

;
-- select unique instrument and title and order by earliest event release
with order_by_dt as
( 
select * , row_number() over (partition by instrument,title order by publisheddtutc asc) rownum 
from keys.[dbo].[MarketAbuseEquityNews] 
where publisheddtutc > @reportdt_start
	and publisheddtutc <= @reportdt_end
)
-- only select earliest event release for duplicate news event titles per instrument
select *
into #MarketAbuseEquityNews_mindt
from order_by_dt
where rownum=1
order by instrument,title



if object_id('tempdb..#outside_mkt_prices') is not null
	begin
			drop table #outside_mkt_prices
	end

;

-- do rownum() over sumbol order by time to get price closest to closing/ oepning time
-- get prevous day eod prices
with peod as
(
-- some prices are missing for eactly at open/close times so we pick the price at the time closest to the open/close time
select t.instrument [symbol], p.bid_c, p.ask_c, row_number() over (partition by t.instrument order by p.time_est desc) rownum
from #octimes t
	left join  keys..PriceDataStockMinOHLC p on isnull(t.basket, t.instrument)= p.symbol
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) <= cast( t.closingtimeutc as time)
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) >= cast( dateadd(minute,-3,t.closingtimeutc) as time)
						
where date=cast(@reportdt_start as date)
and closingtimeutc is not null
)


,csod as
(
-- get current day start of day prices
select t.instrument[symbol], p.bid_o, p.ask_o, row_number() over (partition by t.instrument order by p.time_est asc) rownum
from #octimes t
	left join  keys..PriceDataStockMinOHLC p on  isnull(t.basket, t.instrument)= p.symbol
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) >= cast(t.openingtimeutc as time)
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) <= cast(dateadd(minute,3,t.openingtimeutc) as time)
where date=cast(@reportdt_end as date)
and t.openingtimeutc is not null
)
,ceod as
(
-- get current day end of day prices
select t.instrument [symbol], p.bid_c, p.ask_c, row_number() over (partition by t.instrument order by p.time_est desc) rownum
from #octimes t
	left join  keys..PriceDataStockMinOHLC p on isnull(t.basket, t.instrument)= p.symbol
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) <= cast(t.closingtimeutc as time)
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) >= cast(dateadd(minute,-3,t.closingtimeutc) as time)
where date=cast(@reportdt_end as date)
and t.closingtimeutc is not null
)
,csod_1hrmax as
(
--get current max bid hi and min bid lo 1hr after market opens
select t.instrument [symbol], max(p.bid_hi) [bid_hi_1hr], min(p.bid_lo) [bid_lo_1hr], max(p.ask_hi) [ask_hi_1hr], min(p.ask_lo) [ask_lo_1hr]
from #octimes t
	left join  keys..PriceDataStockMinOHLC p on isnull(t.basket, t.instrument)= p.symbol
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) > cast( t.openingtimeutc as time)
											and cast(dateadd(hour,t.UTCOffset,p.time_est) as time) < cast( dateadd(hour,1,t.openingtimeutc) as time)
where date=cast(@reportdt_end as date)
and t.closingtimeutc is not null
group by t.instrument
)

select t.instrument [symbol], t.basket, peod.bid_c [peod_bid_c], peod.ask_c [peod_ask_c],csod.bid_o [csod_bid_o],csod.ask_o [csod_ask_o]
,ceod.bid_c [ceod_bid_c], ceod.ask_c [ceod_ask_c], csod_1hrmax.bid_hi_1hr [csod1hr_bid_hi], csod_1hrmax.bid_lo_1hr [csod1hr_bid_lo]
, csod_1hrmax.ask_hi_1hr [csod1hr_ask_hi], csod_1hrmax.ask_lo_1hr [csod1hr_ask_lo]--,peod.rownum [peod_row], csod.rownum [csod_row], peod.rownum [ceod_row]
into #outside_mkt_prices
from  #octimes t
	left join peod on t.instrument = peod.symbol
					and peod.rownum =1
	left join csod on t.instrument = csod.symbol
					and csod.rownum =1
	left join ceod on t.instrument = ceod.symbol
					and ceod.rownum =1
	left join csod_1hrmax on t.instrument = csod_1hrmax.symbol

--select @reportdt_start, @reportdt_end



if object_id('tempdb..#pricestats') is not null
					drop table #pricestats;

;
with pricedata as
(
select  m.id,m.publisheddtutc,t.openingtimeutc,t.closingtimeutc , m.instrument, m.companyname, m.basket , m.sentiment, m.title
--PRICE AT TIME OF NEWS

-- when date of news is day before or time of news is before market open i.e (between 16:00- 09:30) use market close price of day before 
-- when after market close current day use current eod prices
, case when (cast(m.publisheddtutc as date) = cast(@reportdt_start as date) or  cast(m.publisheddtutc as time) < t.openingtimeutc) -- if event is before market open use prev eod close price
				then omp.peod_bid_c 
		when (cast(m.publisheddtutc as date) =  cast(@reportdt_end as date) and cast(m.publisheddtutc as time) > t.closingtimeutc) -- if event is after market close on current day use curr eod close price
					then omp.ceod_bid_c
		else -- else get the price at time of event
			 p3.bid_o 
	end bid_o

, case when (cast(m.publisheddtutc as date) = cast(@reportdt_start as date) or cast(m.publisheddtutc as time) < t.openingtimeutc)
				then omp.peod_ask_c 
		when (cast(m.publisheddtutc as date) =  cast(@reportdt_end as date) and cast(m.publisheddtutc as time) > t.closingtimeutc)
					then omp.ceod_ask_c
		else
			p3.ask_o 
	end ask_o
--PRICE 1HR FOLLOWING NEWS or after market opens if outside market hour
--bid high 
, case when (cast(m.publisheddtutc as date) = cast(@reportdt_start as date) or cast(m.publisheddtutc as time) < t.openingtimeutc)
			then omp.csod1hr_bid_hi -- if before market open then get 1hr max after market open
		when (cast(m.publisheddtutc as date) = cast(@reportdt_end as date) and   cast(m.publisheddtutc as time) > t.closingtimeutc )
			then omp.ceod_bid_c -- if after current end of day get end of day close price
		else p.m_bid_hi -- otherwise use bid hi price
	end bid_hi
--bid low
, case when (cast(m.publisheddtutc as date) = cast(@reportdt_start as date) or cast(m.publisheddtutc as time) < t.openingtimeutc)
			then omp.csod1hr_bid_lo
		when (cast(m.publisheddtutc as date) = cast(@reportdt_end as date) and   cast(m.publisheddtutc as time) > t.closingtimeutc )
			then omp.ceod_bid_c  
		else p.m_bid_lo
	end bid_lo
--ask high
, case when (cast(m.publisheddtutc as date) = cast(@reportdt_start as date) or cast(m.publisheddtutc as time) < t.openingtimeutc)
			then omp.csod1hr_ask_hi
		when (cast(m.publisheddtutc as date) = cast(@reportdt_end as date) and   cast(m.publisheddtutc as time) > t.closingtimeutc )
			then omp.ceod_ask_c  
		else p.m_ask_hi
	end ask_hi
-- ask low
, case when (cast(m.publisheddtutc as date) = cast(@reportdt_start as date) or cast(m.publisheddtutc as time) < t.openingtimeutc)
			then omp.csod1hr_ask_lo
		when (cast(m.publisheddtutc as date) = cast(@reportdt_end as date) and   cast(m.publisheddtutc as time) > t.closingtimeutc )
			then omp.ceod_ask_c  
		else p.m_ask_lo
	end ask_lo
, s.mean,s.median,s.mad,s.std
from #MarketAbuseEquityNews_mindt m
--open and close times and utc offset hours
left join #octimes t on case when m.basket is null 
								then m.instrument 
							else m.instrument
						end like t.instrument
--high and lows an hour after event per event
left join (select m2.id, p2.symbol, max(p2.bid_o) [m_bid_o], max(p2.bid_hi) [m_bid_hi], max(p2.bid_lo) [m_bid_lo], max(p2.bid_c) [m_bid_c], max(p2.ask_o) [m_ask_o], max(p2.ask_hi) [m_ask_hi], max(p2.ask_lo) [m_ask_lo], max(p2.ask_c) [m_ask_c]
			from #MarketAbuseEquityNews_mindt m2
			left join #octimes t2 on case when m2.basket is null 
											then m2.instrument
										else m2.instrument
									end = t2.instrument
			left join keys..PriceDataStockMinOHLC p2 on m2.publisheddtutc <= convert(datetime,cast(p2.date as varchar) +' '+ cast(left(dateadd(hour,t2.UTCOffset,p2.time_est),12) as varchar(12)) ,21) 
														and	dateadd(hh,1,m2.publisheddtutc) >= convert(datetime,cast(p2.date as varchar) +' '+ cast(left(dateadd(hour,t2.UTCOffset,p2.time_est),12) as varchar(12)),21) 
														and p2.symbol = case when m2.basket is null -- basket prices are used not underlying stock
																				then m2.instrument
																			else m2.basket 
																		end 

			 group by m2.id, p2.symbol
			) as p on p.id = m.id
--prices at time of event per event
left join keys..PriceDataStockMinOHLC p3 on p3.symbol like case when m.basket is null -- basket prices are used not underlying stock
													then m.instrument
												else m.basket 
											end  
											and m.publisheddtutc = convert(datetime,cast(p3.date as varchar) +' '+ cast(left(dateadd(hour,t.UTCOffset,p3.time_est),12) as varchar(12)) ,21) 
--previous day end of day prices, current day start of day prices, current day end of day prices
left join #outside_mkt_prices omp on  omp.symbol like case when m.basket is null 
																then m.instrument 
															else m.instrument
														end -- price move already calculated based on basket so link to events by instrument
--mad,std figures
left join keys..MarketAbuseStockStats s on  s.symbol like case when m.basket is null
																	then m.instrument
																else m.basket
															end -- price move already calculated based on basket, stats data has baskets as symbol
											and cast(m.publisheddtutc as varchar(11)) = s.rundate

where s.mad <> 0 
)

select *
into #pricestats
from pricedata 
group by  id, publisheddtutc,openingtimeutc,closingtimeutc , instrument, companyname, basket , sentiment, title, bid_o, ask_o, bid_hi, ask_hi, bid_lo, ask_lo, mean, median, mad, std
order by id asc





set @mon = month(@reportdt_start)
set @year = year(@reportdt_start)

set @monthyyyy = (select datename(mm,cast(@mon as varchar(2))+'/1/1900')+@year)


set @yyyymm =(select @year + right('0' + cast(@mon as varchar(2)), 2)) 

Declare @Prev_Month as datetime =  dateadd(month,-1, @reportdt_start)
set @yyyymm_mon =(select cast(year(@Prev_Month) as varchar(4)) + right('0' + cast(month(@Prev_Month) as varchar(2)), 2) + '_' 
	
												  + left(datename(mm,cast(month(@Prev_Month) as varchar(2))+'/1/1900'),3))
											

set @yyyymmddt = cast(convert(varchar(12),@rundt,121) as varchar(11)) + '16:59:59'

declare @p_yyyymmdd varchar(10)= convert(varchar(10), dateadd(day,-1,@rundt),121)


if object_id('tempdb..#eod_ot') is not null
	begin
		drop table  #eod_ot 
	end;
--select @yyyymmddt

--open trades pnl cal for day before if not present on curr day
create table #eod_ot (
report_dt datetime null,
trade_id nvarchar(50) null,
report_id nvarchar(50) null,
fpnl decimal(20,5) null
)
declare @ot varchar(max) = '
insert into  #eod_ot 
select ot.report_dt,ot.trade_id,ot.report_id,ot.fpnl
from reports..global_ot ot
	left join #octimes t on t.instrument = ot.symbol
where t.instrument is not null 
and report_dt = ''<yyyymmddt>''
'
set @ot=replace(@ot,'<yyyymmddt>',@yyyymmddt);
exec(@ot)

--load curr day for ct when finding open trades that are closed before event

if object_id('tempdb..#ct') is not null
	begin
		drop table  #ct 
	end;

create table #ct (
server_name nvarchar(50) null ,
report_id nvarchar(30) null,
cls_dt datetime null,
symbol nvarchar(32) null,
quantity float null,
opn_dt datetime null,
opn_rate float null,
bs nvarchar(1) null,
report_dt datetime null,

)



declare @ct varchar(max) = '
insert into #ct
select  ct.server_name, ct.report_id, ct.cls_dt, ct.symbol, sum(ct.quantity) quantity, ct.opn_dt, ct.opn_rate, ct.bs, ct.report_dt
from reports..global_ct ct
	left join #octimes t on t.instrument = ct.symbol
where t.instrument is not null 
and report_dt = ''<yyyymmddt>''
group by  ct.server_name, ct.report_id, ct.cls_dt, ct.symbol, ct.opn_dt, ct.opn_rate, ct.bs, ct.report_dt
'
set @ct=replace(@ct,'<yyyymmddt>',@yyyymmddt);
exec(@ct)




-- select thresholds into temp and select individual thresholds as variables inside @executed variable to save from converting from int to varchar
-- for every threshold
if object_id('tempdb..#thresholds') is not null
	begin
		drop table  #thresholds
	end;

select *, row_number() over (partition by alert,criteria order by threshold_date desc) rownum
into #thresholds
from keys..marketabusethresholds
where alert like 'TradingAheadOfNewsEvent'


--EXECUTED ORDERS OT & CT
declare @executed varchar(max)=
'
declare @z_score decimal(4,2) = (select threshold from #thresholds where criteria like ''z-score'' and rownum=1)

declare @PriceMove1 int = (select threshold from #thresholds where criteria like ''PriceMove1'' and rownum=1)
declare @PriceMove2 int = (select threshold from #thresholds where criteria like ''PriceMove2'' and rownum=1)

declare @Notional_Lower int = (select threshold from #thresholds where criteria like ''NotionalVol1'' and rownum=1)
declare @Notional_Upper int = (select threshold from #thresholds where criteria like ''NotionalVol2'' and rownum=1)

declare @Pnl1 int = (select threshold from #thresholds where criteria like ''Pnl1'' and rownum=1)
declare @Pnl2 int = (select threshold from #thresholds where criteria like ''Pnl2'' and rownum=1)


; with open_trades as
(
select ''018-''+ convert(nvarchar(255), newid()) [AlertID],''<yyyymmddt>'' report_dt, p.Publisheddtutc, p.instrument, p.companyname, p.basket, p.sentiment,p.title
, ot.server_name, ot.report_id, h.ownername, ot.buysell, ''open'' [opened/closed] 

, case when ot.buysell like ''B'' -- if buy then close out the trade with a sell so we use bid - want to identify the greatest profit so  we use bid_hi
			then cast( (( (p.bid_o - p.bid_hi)/p.bid_hi )*100) as decimal(8,3)) 
		when ot.buysell like ''S'' 
			then cast( (( (p.ask_o - p.ask_lo)/p.ask_lo )*100) as decimal(8,3)) end  [price_move_pcd]
, case when ot.buysell like ''B'' 
			then cast (( (( ((p.bid_o - p.bid_hi)/p.bid_hi )*100)) - p.median)/p.mad as decimal (8,3)) 
		when ot.buysell like ''S'' 
			then cast (( (( ( (p.ask_o - p.ask_lo)/p.ask_lo )*100)) - p.median)/p.mad as decimal (8,3)) end [z_score]
	
, ot.trade_id,ot.OPEN_DT
, case when ot.base_ccy like ''USD''
		then 
			case when ( p.publisheddtutc < ''<p_yyyymmdd> '' + cast(oc.closingtimeutc as varchar(11) ) ) and ot.symbol like ''%.us%''
				then ot.fpnl
			when  ( p.publisheddtutc < ''<p_yyyymmdd> '' + cast(oc.closingtimeutc as varchar(11) ) ) and (ot.symbol like ''%.uk%'' or ot.symbol like ''%.de%'' or ot.symbol like ''%.fr%'')
				then ot.fpnl
			else isnull(eod.fpnl,ot.fpnl) end
	else 
		case when ( p.publisheddtutc < ''<p_yyyymmdd> '' + cast(oc.closingtimeutc as varchar(11)) ) and ot.symbol like ''%.us%''
				then ot.fpnl * usd.stk
			when ( p.publisheddtutc < ''<p_yyyymmdd> '' + cast(oc.closingtimeutc as varchar(11)) ) and (ot.symbol like ''%.uk%'' or ot.symbol like ''%.de%'' or ot.symbol like ''%.fr%'')
				then ot.fpnl * usd.stk
			else isnull(eod.fpnl * usd.stk,ot.fpnl * usd.stk)  end
	end fpnl_usd
,  ot.quantity*cy.rate [notional vol]
, be.entity, ot.quantity,ot.open_price
from reports..global_ot ot
	left join #pricestats p on p.instrument = ot.symbol
						and p.Publisheddtutc <=dateadd(dd,10, ot.open_dt)
	left join keys..booklist_integration b on b.Book = ot.book_name
	left join '+@hdm_file_name+' h on h.id = ot.report_id and h.server=ot.server_name
	left join #octimes oc on oc.instrument =  ot.symbol
	left join  keys..cy_<yyyymm_mon> cy on cy.currency = p.instrument
	left join keys..usd_<yyyymm_mon> usd on usd.base = ot.base_ccy
	left join #eod_ot eod on eod.TRADE_ID = ot.TRADE_ID and eod.REPORT_ID = ot.REPORT_ID
	left join keys..book_entity be on be.book = ot.book_name
where p.Publisheddtutc is not null
and ot.report_dt = ''<p_yyyymmdd> 16:59:59''
and sign(p.sentiment) =  case when ot.buysell like ''B'' 
							then sign((p.bid_o - p.bid_hi)/p.bid_hi) 
						when ot.buysell like ''S'' 
							then sign ((p.ask_o - p.ask_lo)/p.ask_lo) end 
and ot.buysell like case when sign(p.sentiment) = -1 then ''S''
			else ''B'' end
and ot.acct_test_flg like ''N''
and b.filter=''yes''
)

,true_open as -- open trades still open after news release, not closed before event
( --aggreagate over unique trade identifiers to sum over partial closures
select ot.*
 from open_trades ot
			left join #ct ct on ot.server_name = ct.server_name 
									and ot.report_id = ct.report_id
									and ot.instrument = ct.symbol
									and ot.open_dt = ct.opn_dt
									and ot.open_price = ct.OPN_RATE
									and ot.buysell = ct.bs
									and ct.report_dt = ''<yyyymmddt>''

where ct.report_id is null -- fully open
or (ct.cls_dt > ot.publisheddtutc) -- still open after event
or (ct.cls_dt < ot.publisheddtutc and ct.quantity < ot.quantity) --trades can still be partially closed before the event, we stil want those as they are not fully closed
)



insert into project..marketabuse_insiderdealing_tradingaheadofnews
select  [AlertID],report_dt, Publisheddtutc, instrument, companyname, basket, sentiment,title
, server_name, report_id, ownername, buysell,  [opened/closed] 
, [price_move_pcd]
,[z_score]
, trade_id,OPEN_DT
, fpnl_usd
,  [notional vol]
, entity
from true_open ot
where abs(z_score) > @z_score
--price move threshold
and abs([price_move_pcd]) > case when abs([notional vol])  >= @Notional_Lower and abs([notional vol]) <= @Notional_Upper
										then @PriceMove1
									when [notional vol] > @Notional_Upper
										then @PriceMove2 end
-- pnl threshold
and abs(fpnl_usd) > case when abs([notional vol])  >= @Notional_Lower and abs([notional vol]) <= @Notional_Upper
										then @Pnl1
									when [notional vol] > @Notional_Upper
										then @Pnl2 end


; with closed_trades as
(
select ''018-''+ convert(nvarchar(255), newid()) [AlertID],''<yyyymmddt>'' report_dt, p.Publisheddtutc, p.instrument, p.companyname, p.basket, p.sentiment,p.title
, ct.server_name, ct.report_id, h.ownername, ct.bs,''closed'' [opened/closed]

, case when ct.bs like ''B'' -- if buy then close out the trade with a sell so we use bid - want to identify the greatest profit so  we use bid_hi
			then cast( (( (p.bid_o - p.bid_hi)/p.bid_hi )*100) as decimal(8,3)) 
		when ct.bs like ''S'' 
			then cast( (( (p.ask_o - p.ask_lo)/p.ask_lo )*100) as decimal(8,3)) end  [price_move_pcd]
, case when ct.bs like ''B'' 
			then cast (( (( ((p.bid_o - p.bid_hi)/p.bid_hi )*100)) - p.median)/p.mad as decimal (8,3)) 
		when ct.bs like ''S'' 
			then cast (( (( ( (p.ask_o - p.ask_lo)/p.ask_lo )*100)) - p.median)/p.mad as decimal (8,3)) end [z_score]

, ct.trade_id, ct.opn_dt
, case when ct.base_ccy like ''USD''
			then ct.GROSS_PL
		else ct.GROSS_PL *usd.stk
	end fpnl_usd
,  ct.quantity*cy.rate  [notional vol]
, be.entity
from reports..global_ct ct
	left join #pricestats p on p.instrument = ct.symbol
						and  p.Publisheddtutc <=dateadd(dd,10, ct.cls_dt)
						and  p.Publisheddtutc >= dateadd(m,2,ct.opn_dt)
	left join keys..booklist_integration b on b.Book = ct.book_name
	left join '+@hdm_file_name+' h on h.id = ct.report_id and h.server=ct.server_name
	left join  keys..cy_<yyyymm_mon> cy on cy.currency = p.instrument
	left join keys..usd_<yyyymm_mon> usd on usd.base = ct.base_ccy
	left join keys..book_entity be on be.book = ct.book_name
where p.Publisheddtutc is not null
and sign(p.sentiment) =   case when ct.bs like ''B'' 
							then sign((p.bid_o - p.bid_hi)/p.bid_hi) 
						when ct.bs like ''S'' 
							then sign ((p.ask_o - p.ask_lo)/p.ask_lo) end 
and ct.bs like case when sign(p.sentiment) = -1 then ''S''
					else ''B'' end
and ct.test_flg like ''N''
and b.filter=''yes''

)

insert into project..marketabuse_insiderdealing_tradingaheadofnews
select *
from closed_trades
where abs(z_score) > @z_score
--price move threshold
and abs([price_move_pcd]) > case when abs([notional vol])  >= @Notional_Lower and abs([notional vol]) <= @Notional_Upper
										then @PriceMove1
									when [notional vol] > @Notional_Upper
										then @PriceMove2 end
-- pnl threshold
and abs(fpnl_usd) > case when abs([notional vol])  >= @Notional_Lower and abs([notional vol]) <= @Notional_Upper
										then @Pnl1
									when [notional vol] > @Notional_Upper
										then @Pnl2 end
'

set @executed=replace(@executed,'<yyyymm>',@yyyymm);
set @executed=replace(@executed,'<yyyymm_mon>',@yyyymm_mon)
set @executed=replace(@executed,'<yyyymmddt>',@yyyymmddt);
set @executed=replace(@executed,'<p_yyyymmdd>',@p_yyyymmdd);

exec(@executed)




--CANCELLED PENDING ORDERS

set @mon = month(@reportdt_start)
set @year = year(@reportdt_start)

set @monthyyyy = (select datename(mm,cast(@mon as varchar(2))+'/1/1900')+@year)


set @yyyymm =(select @year + right('0' + cast(@mon as varchar(2)), 2)) 

set @Prev_Month =  dateadd(month,-1, @reportdt_start)
set @yyyymm_mon =(select cast(year(@Prev_Month) as varchar(4)) + right('0' + cast(month(@Prev_Month) as varchar(2)), 2) + '_' 
	
												  + left(datename(mm,cast(month(@Prev_Month) as varchar(2))+'/1/1900'),3))
											

set @yyyymmddt = cast(convert(varchar(12),@rundt,121) as varchar(11)) + '16:59:59'


set @p_yyyymmdd = convert(varchar(10), dateadd(day,-1,@rundt),121)

if object_id('tempdb..#ot') is not null
	begin
		drop table  #ot 
	end;

create table #ot (
report_dt datetime,
server_name nvarchar(50) null ,
report_id nvarchar(30) null,
symbol nvarchar(32) null,
quantity float null,
open_dt datetime null,
open_price float null,
buysell nvarchar(1) null,
cls_dt datetime null,
fpnl decimal(10,2),
[type] nvarchar(10)
)



set @ot = '	
			--get open trades of 1 days prior to rundt
			with ot as 
			(
				select ot.report_dt, ot.server_name, ot.report_id, ot.symbol, sum(ot.quantity) quantity, ot.open_dt, ot.open_price, ot.buysell,sum(ot.fpnl*r.stk) fpnl
				from reports..global_ot ot
					left join keys..booklist_integration bi on bi.book = ot.book_name
					left join keys..usd_<yyyymm_mon> r on r.base = ot.base_ccy
					left join (select min(publisheddtutc) publisheddtutc, instrument 
								from #pricestats 
								group by instrument

								union 

								select min(publisheddtutc) publisheddtutc, basket 
								from #pricestats 
								where basket is not null
								group by basket) news on ot.symbol = news.instrument
															
				where ( 
						report_dt <= ''<rundt> 16:59:59'' 
						and dateadd(dd, 11 , report_dt) >= ''<rundt> 16:59:59'' 
						)
				and bi.filter like ''yes''
				and news.instrument is not null -- only pick instruments needed for that day
				and ot.acct_test_flg like ''N''
				group by ot.report_dt, ot.server_name, ot.report_id, ot.symbol, ot.open_dt, ot.open_price, ot.buysell

			)	
			--get floating pnls 
			, ot2 as ( 
						select ot.*,o2.fpnl2 
							from ot ot
								left join ( select dateadd(dd,1,o.report_dt) report_dt, o.server_name, o.report_id, o.symbol,  o.open_dt, o.open_price, o.buysell,sum(fpnl*r.stk) fpnl2
												from reports..global_ot o
													left join keys..usd_<yyyymm_mon> r on r.base = o.base_ccy
													left join keys..booklist_integration bi on bi.book = o.book_name
												where bi.filter like ''yes''
													and o.acct_test_flg like  ''N''
													and (dateadd(dd,1,o.report_dt) = ''<rundt> 16:59:59''
													or dateadd(dd,2,o.report_dt) = ''<rundt> 16:59:59'')
												group by   dateadd(dd,1,o.report_dt) , o.server_name, o.report_id, o.symbol,  o.open_dt, o.open_price, o.buysell
											) o2 on ot.server_name = o2.server_name 
													and ot.report_id = o2.report_id
													and ot.symbol = o2.symbol
													and ot.open_dt = o2.open_dt
													and ot.open_price = o2.open_price
													and ot.buysell = o2.buysell
													and ot.report_dt = o2.report_dt
					)

			,ct as(
					select  ct.report_dt, ct.server_name, ct.report_id, max(ct.cls_dt) cls_dt, ct.symbol, sum(ct.quantity) quantity, ct.opn_dt, ct.opn_rate, ct.bs, sum(gross_pl*r.stk) gross_pl
					from reports..global_ct ct
						left join keys..booklist_integration bi on bi.book = ct.book_name
						left join keys..usd_<yyyymm_mon> r on r.base = ct.base_ccy
						left join (select min(publisheddtutc) publisheddtutc, instrument 
								from #pricestats 
								group by instrument

								union 

								select min(publisheddtutc) publisheddtutc, basket 
								from #pricestats 
								where basket is not null
								group by basket) news on ct.symbol = news.instrument
															

					where news.instrument is not null 
					and report_dt  <= ''<rundt> 16:59:59''
					and dateadd(dd, 11 , report_dt)  >= ''<rundt> 16:59:59''
					and bi.filter like ''yes''
					and test_flg like ''N''
					group by ct.report_dt, ct.server_name, ct.report_id, ct.symbol, ct.opn_dt, ct.opn_rate, ct.bs
			)
	

			-- Get all open positions and when they were fully closed out if they were
			
			insert into #ot
			select  ot.report_dt,
			ot.server_name, ot.report_id, ot.symbol,  ot.quantity, ot.open_dt, ot.open_price, ot.buysell, ct.cls_dt
				, fpnl-isnull(fpnl2,0) fpnl -- if client didnt have that position the day before then fpnl is just that days fpnl
				,''open'' [type]
			from  ot2 ot
				left join  ct on ot.server_name = ct.server_name 
									and ot.report_id = ct.report_id
									and ot.symbol = ct.symbol
									and ot.open_dt = ct.opn_dt
									and ot.open_price = ct.OPN_RATE
									and ot.buysell = ct.bs
									and ot.report_dt <= ct.report_dt
									and ct.quantity >= ot.quantity --fully closed

			union 

			-- get intra day trades fully closed trades
			select ct.report_dt, 
			ct.server_name, ct.report_id, ct.symbol, ct.quantity, ct.opn_dt, ct.opn_rate, ct.bs,ct.cls_dt,gross_pl,
			case when cast(ct.cls_dt as date) = cast(ct.opn_dt as date) then ''intra'' else ''closed'' end [type]
			from ct 
				left join ot2 ot on ot.server_name = ct.server_name 
									and ot.report_id = ct.report_id
									and ot.symbol = ct.symbol
									and ot.open_dt = ct.opn_dt
									and ot.open_price = ct.OPN_RATE
									and ot.buysell = ct.bs
									and ot.report_dt<=ct.report_dt -- fully closed trades that wont show up in that days open trades table
			where cast(ct.cls_dt as date) = cast(ct.opn_dt as date) -- get intra day trades
			or ot.symbol is null or ct.quantity >= ot.quantity --  fully closed out trades

		
			'

		set @ot = replace(@ot,'<rundt>',convert(nvarchar(10),@rundt,121))
		set @ot=replace(@ot,'<yyyymm_mon>',@yyyymm_mon)
		exec(@ot)



	
	
declare @cpo nvarchar(max) ='
				declare @z_score decimal(4,2) = (select threshold from #thresholds where criteria like ''z-score'' and rownum=1)


				--downward price move, cancelled buy,executed sell
				insert into  project.[dbo].[marketabuse_insiderdealing_tradingaheadofnews_cpo]
				select ''019-''+ convert(nvarchar(255), newid()) [AlertID],''<yyyymmddt>'' report_dt,v.server, v.reportid
				, isnull(lam.first_name,'''') + '' '' + isnull(lam.last_name,'''') clientname
				,v.executeddate,orderid,news.instrument,v.remainingqty PO_Lots, abs(v.originqty)*r.rate PO_Notional_Volume, ot.quantity *r.rate notional_volume,be.entity
				,case when v.currency = news.basket then news.basket else null end basket, v.side, ot.open_dt, ot.open_price
				, case when v.side like ''B''
							then cast( (( (p.bid_o - p.bid_hi)/p.bid_hi )*100) as decimal(8,3)) 
						when v.side like ''S'' 
							then cast( (( (p.ask_o - p.ask_lo)/p.ask_lo )*100) as decimal(8,3)) end  [price_move_pct]
				, ot.cls_dt, ot.fpnl, news.publisheddtutc, news.sentiment, news.title
				, p.bid_hi, v.rate, abs(p.bid_hi-v.rate) rate_diff, s.std
				,case when sb.symbol is null then ds.high else sb.high end day_hi,''DBS'' [description]
				from volume..<monthyyyy> v
					left join keys..marketabusestockstats s on s.symbol = v.currency   -- for standard dev
															and s.rundate = cast(executeddate as date)
					left join keys..pricedatastockminohlc p on p.symbol  = v.currency -- for stock prices at the time of cancellation
															and cast(v.executeddate as date) = p.date
															and cast(dateadd(s,-datepart(s,executeddate),executeddate) as time) = p.time_est
					left join keys..pricedatastockbasket sb on sb.symbol = v.currency 
															and cast(v.executeddate as date) = sb.date -- for basket prices at the time of cancellation
					left join keys..pricedatastock ds on ds.ticker = v.currency  
																	and ds.date = cast(v.executeddate as date) -- to see if stock price would have gone above cancellation rate
					left join #pricestats news on v.currency = case when sb.symbol is null then news.instrument else news.basket end
											and v.executeddate  >= dateadd(dd,-10,news.publisheddtutc) -- order cancelled up to 10 days before
											and v.executeddate < news.publisheddtutc -- order has to be executed before time of event
					left join #ot ot on ot.server_name = v.server
									and ot.report_id = v.reportid
									and ot.symbol = v.currency
									and v.executeddate>=ot.open_dt -- cancelled orders have to be on opened trades at the time
									and v.executeddate < isnull(ot.cls_dt,getdate()) -- trade has to be not be closed at time of event 
									and v.creationdate>=ot.open_dt -- open trade has to be open at time of creation
									and v.creationdate < isnull(ot.cls_dt,getdate()) -- open trade has to be open at time of creation
									and ot.open_dt > dateadd(dd,-10,v.executeddate) -- trade has to be opened 10 days before time of event 
									and (ot.report_dt=case when cast(news.publisheddtutc as date) = ''<rundt>'' and cast(news.publisheddtutc as time) <= ''16:59:59'' then dateadd(dd,-1,''<rundt> 16:59:59'')
															when cast(news.publisheddtutc as date) = dateadd(dd,-1,''<rundt>'') then dateadd(dd,-1,''<rundt> 16:59:59'')
															when cast(news.publisheddtutc as date) = ''<rundt>'' and cast(news.publisheddtutc as time) >= ''16:59:59'' then ''<rundt> 16:59:59'' end -- pick the right report_dt based on when the TE occured
										 or (ot.report_dt= convert(nvarchar(10),v.executeddate,121)+ '' 16:59:59'' and ot.type = ''intra'')) --intra day closure
					left join keys..loginacctmapping lam on lam.report_id = v.reportid and lam.server = v.server
														and lam.report_Dt  = ''<yyyymmddt>''
					left join keys..book_entity be on be.book = lam.book_name
					left join keys..cy_<yyyymm_mon> r on r.currency = v.currency
				where v.filter like ''yes''
				and v.exaction like ''delete''
				and v.testflag <> ''y'' 
				and v.accounttype <> ''clearing'' 
				and v.accounttype <> ''40''
				and news.sentiment is not null
				and news.sentiment < 0 -- downward price move
				and ot.symbol is not null
				where abs(case when ct.bs like ''B'' 
									then cast (( (( ((p.bid_o - p.bid_hi)/p.bid_hi )*100)) - p.median)/p.mad as decimal (8,3)) 
								when ct.bs like ''S'' 
									then cast (( (( ( (p.ask_o - p.ask_lo)/p.ask_lo )*100)) - p.median)/p.mad as decimal (8,3)) end [z_score]) 
							 > @z_score
				and ot.buysell like ''S'' -- executed sell
				and ot.open_dt <= news.publisheddtutc  -- trade has to be opened before the event
				and isnull(ot.cls_dt,getdate()) > news.publisheddtutc -- trade has to be opened before the event
				and v.side like ''B'' -- cancelled buy
				and v.rate < ot.open_price -- cancellation price of the buy has to be < the sell open_price
				and abs(p.bid_hi-v.rate)<=s.std -- rate at time of cancellation has to be < 1 std from the pending order rate
				and v.rate < case when sb.symbol is null then ds.high else sb.high end  -- price of stock has to have gone higher than the cancellation rate for the client to have made profit.


				--upward price move, cancelled sell ,executed buy
				insert into  project.[dbo].[marketabuse_insiderdealing_tradingaheadofnews_cpo]
				select ''019-''+ convert(nvarchar(255), newid()) [AlertID],''<yyyymmddt>'' report_dt,v.server, v.reportid
				, isnull(lam.first_name,'''') + '' '' + isnull(lam.last_name,'''') clientname
				,v.executeddate,orderid,news.instrument,v.remainingqty PO_Lots, abs(v.originqty)*r.rate PO_Notional_Volume, ot.quantity *r.rate notional_volume,be.entity
				,case when v.currency = news.basket then news.basket else null end basket, v.side, ot.open_dt, ot.open_price
				, case when v.side like ''B''
						then cast( (( (p.bid_o - p.bid_hi)/p.bid_hi )*100) as decimal(8,3)) 
					when v.side like ''S'' 
						then cast( (( (p.ask_o - p.ask_lo)/p.ask_lo )*100) as decimal(8,3)) end  [price_move_pct]
				, ot.cls_dt, ot.fpnl, news.publisheddtutc, news.sentiment, news.title
				, p.bid_hi, v.rate, abs(p.bid_hi-v.rate) rate_diff, s.std
				,case when sb.symbol is null then ds.high else sb.high end day_hi,''USB'' [description]
			from volume..<monthyyyy> v
					left join keys..marketabusestockstats s on s.symbol =  v.currency  -- for standard dev
															and s.rundate = cast(executeddate as date)
					left join keys..pricedatastockminohlc p on p.symbol  = v.currency -- for stock prices at the time of cancellation
															and cast(v.executeddate as date) = p.date
															and cast(dateadd(s,-datepart(s,executeddate),executeddate) as time) = p.time_est
					left join keys..pricedatastockbasket sb on sb.symbol = v.currency and cast(v.executeddate as date) = sb.date -- to see if basket price would have gone above cancellation rate
					left join keys..pricedatastock ds on ds.ticker =  v.currency  
													and ds.date = cast(v.executeddate as date) -- to see if stock price would have gone above cancellation rate
					left join #pricestats news on v.currency = case when sb.symbol is null then news.instrument else news.basket end
												and v.executeddate  >= dateadd(dd,-10,news.publisheddtutc) -- order cancelled up to 10 days before
												and v.executeddate < news.publisheddtutc -- order has to be executed before time of event
												
					left join #ot ot on ot.server_name = v.server
									and ot.report_id = v.reportid
									and ot.symbol = v.currency
									and v.executeddate>=ot.open_dt -- cancelled orders have to be on opened trades at the time
									and v.executeddate < isnull(ot.cls_dt,getdate()) -- trade has to be not be closed at time of event 
									and v.creationdate>=ot.open_dt -- open trade has to be open at time of creation
									and v.creationdate < isnull(ot.cls_dt,getdate()) -- open trade has to be open at time of creation
									and ot.open_dt > dateadd(dd,-10,v.executeddate) -- trade has to be opened 10 days before time of event 
									and (ot.report_dt=case when cast(news.publisheddtutc as date) = ''<rundt>'' and cast(news.publisheddtutc as time) <= ''16:59:59'' then dateadd(dd,-1,''<rundt> 16:59:59'')
															when cast(news.publisheddtutc as date) = dateadd(dd,-1,''<rundt>'') then dateadd(dd,-1,''<rundt> 16:59:59'')
															when cast(news.publisheddtutc as date) = ''<rundt>'' and cast(news.publisheddtutc as time) >= ''16:59:59'' then ''<rundt> 16:59:59'' end -- pick the right report_dt based on when the TE occured
										 or (ot.report_dt= convert(nvarchar(10),v.executeddate,121)+ '' 16:59:59'' and ot.type = ''intra'')) --intra day closure
					left join keys..loginacctmapping lam on lam.report_id = v.reportid and lam.server = v.server
														and lam.report_Dt  = ''<yyyymmddt>''
					left join keys..book_entity be on be.book = lam.book_name
					left join keys..cy_<yyyymm_mon> r on r.currency = v.currency
				where v.filter like ''yes''
				and news.sentiment is not null
				and news.sentiment > 0 --upward price move
				where abs(case when ct.bs like ''B'' 
									then cast (( (( ((p.bid_o - p.bid_hi)/p.bid_hi )*100)) - p.median)/p.mad as decimal (8,3)) 
								when ct.bs like ''S'' 
									then cast (( (( ( (p.ask_o - p.ask_lo)/p.ask_lo )*100)) - p.median)/p.mad as decimal (8,3)) end [z_score]) 
							 > @z_score
				and v.exaction like ''delete''
				and v.testflag <> ''y'' 
				and v.accounttype <> ''clearing'' 
				and v.accounttype <> ''40''
				and ot.symbol is not null -- only show cancelled orders on opened positions
				and ot.buysell like ''B'' -- executed buy
				and ot.open_dt <= news.publisheddtutc  -- trade has to be opened before the event
				and isnull(ot.cls_dt,getdate()) > news.publisheddtutc -- trade has to be opened before the event
				and v.side like ''S'' -- cancelled sell
				and v.rate > ot.open_price -- cancellation price of the sell has to be > the buy open_price
				and abs(p.bid_hi-v.rate)<=s.std -- rate at time of cancellation has to be < 1std from the pending order rate
				and v.rate > case when sb.symbol is null then ds.high else sb.high end  -- price of stock has to have gone lower than the cancellation rate for the client to have made profit.


				-- downward price move - avoidance of loss executed buy, cancelled buy
				insert into  project.[dbo].[marketabuse_insiderdealing_tradingaheadofnews_cpo]
				select ''019-''+ convert(nvarchar(255), newid()) [AlertID],''<yyyymmddt>'' report_dt,v.server, v.reportid
				, isnull(lam.first_name,'''') + '' '' + isnull(lam.last_name,'''') clientname
				,v.executeddate,orderid,news.instrument,v.remainingqty PO_Lots, abs(v.originqty)*r.rate PO_Notional_Volume, ot.quantity *r.rate notional_volume,be.entity
				,case when v.currency = news.basket then news.basket else null end basket, v.side, ot.open_dt, ot.open_price
				, case when v.side like ''B''
						then cast( (( (p.bid_o - p.bid_hi)/p.bid_hi )*100) as decimal(8,3)) 
					when v.side like ''S'' 
						then cast( (( (p.ask_o - p.ask_lo)/p.ask_lo )*100) as decimal(8,3)) end  [price_move_pct]
				, ot.cls_dt, ot.fpnl, news.publisheddtutc, news.sentiment, news.title
				, p.bid_hi, v.rate, abs(p.bid_hi-v.rate) rate_diff, s.std
				,case when sb.symbol is null then ds.high else sb.high end day_hi,''DBB'' [description]
				from volume..<monthyyyy> v
					left join keys..marketabusestockstats s on s.symbol =  v.currency   -- for standard dev
															and s.rundate = cast(executeddate as date)
					left join keys..pricedatastockminohlc p on p.symbol  = v.currency -- for stock prices at the time of cancellation
															and cast(v.executeddate as date) = p.date
															and cast(dateadd(s,-datepart(s,executeddate),executeddate) as time) = p.time_est
					left join keys..pricedatastockbasket sb on sb.symbol = v.currency and cast(v.executeddate as date) = sb.date -- for basket prices at the time of cancellation
					left join keys..pricedatastock ds on ds.ticker =  v.currency  
													and ds.date = cast(v.executeddate as date) -- to see if stock price would have gone above cancellation rate
					left join #pricestats news on v.currency = case when sb.symbol is null then news.instrument else news.basket end
												and v.executeddate  >= dateadd(dd,-10,news.publisheddtutc) -- order cancelled up to 10 days before
												and v.executeddate < news.publisheddtutc -- order has to be executed before time of event
					left join #ot ot on ot.server_name = v.server
									and ot.report_id = v.reportid
									and ot.symbol = v.currency
									and v.executeddate>=ot.open_dt -- cancelled orders have to be on opened trades at the time
									and v.executeddate < isnull(ot.cls_dt,getdate())
									and (ot.report_dt=case when cast(v.executeddate as time) <= ''16:59:59'' then convert(nvarchar(10),dateadd(dd,-1,v.executeddate),121) + '' 16:59:59''
															when cast(v.executeddate as time) >= ''16:59:59'' then convert(nvarchar(10),v.executeddate,121)+ '' 16:59:59'' end -- pick the right report_dt based on when the TE occured
								
										 or (ot.report_dt= convert(nvarchar(10),v.executeddate,121)+ '' 16:59:59'' and ot.type = ''intra'')) --intra day closure
					left join keys..loginacctmapping lam on lam.report_id = v.reportid and lam.server= v.server
														and lam.report_Dt  = ''<yyyymmddt>''
					left join keys..book_entity be on be.book = lam.book_name
					left join keys..cy_<yyyymm_mon> r on r.currency = v.currency
				where v.filter like ''yes''
				and v.exaction like ''delete''
				and v.testflag <> ''y'' 
				and v.accounttype <> ''clearing'' 
				and v.accounttype <> ''40''
				and v.side like ''B'' -- cancelled sell
				and news.sentiment is not null
				and news.sentiment < 0
				where abs(case when ct.bs like ''B'' 
									then cast (( (( ((p.bid_o - p.bid_hi)/p.bid_hi )*100)) - p.median)/p.mad as decimal (8,3)) 
								when ct.bs like ''S'' 
									then cast (( (( ( (p.ask_o - p.ask_lo)/p.ask_lo )*100)) - p.median)/p.mad as decimal (8,3)) end [z_score]) 
							 > @z_score
				and ot.symbol is null -- only show cancelled orders not on opened positions
				and abs(p.bid_hi-v.rate)<=s.std -- rate at time of cancellation has to be < 1std from the pending order rate
				and v.rate > case when sb.symbol is null then ds.high else sb.high end  -- price of stock has to have gone lower than the cancellation rate for the client to have made profit.


				 --upward price move - avoidance of loss executed sell, cancelled sell
				insert into  project.[dbo].[marketabuse_insiderdealing_tradingaheadofnews_cpo]
				select ''019-''+ convert(nvarchar(255), newid()) [AlertID],''<yyyymmddt>'' report_dt,v.server, v.reportid
				, isnull(lam.first_name,'''') + '' '' + isnull(lam.last_name,'''') clientname
				,v.executeddate,orderid,news.instrument,v.remainingqty PO_Lots, abs(v.originqty)*r.rate PO_Notional_Volume, ot.quantity *r.rate notional_volume,be.entity
				,case when v.currency = news.basket then news.basket else null end basket, v.side, ot.open_dt, ot.open_price
				, case when v.side like ''B''
						then cast( (( (p.bid_o - p.bid_hi)/p.bid_hi )*100) as decimal(8,3)) 
					when v.side like ''S'' 
						then cast( (( (p.ask_o - p.ask_lo)/p.ask_lo )*100) as decimal(8,3)) end  [price_move_pct]
				, ot.cls_dt, ot.fpnl, news.publisheddtutc, news.sentiment, news.title
				, p.bid_hi, v.rate, abs(p.bid_hi-v.rate) rate_diff, s.std
				,case when sb.symbol is null then ds.high else sb.high end day_hi,''USS'' [description]
				from volume..<monthyyyy> v
					left join keys..marketabusestockstats s on s.symbol =  v.currency   -- for standard dev
															and s.rundate = cast(executeddate as date)
					left join keys..pricedatastockminohlc p on p.symbol  = v.currency -- for stock prices at the time of cancellation
															and cast(v.executeddate as date) = p.date
															and cast(dateadd(s,-datepart(s,executeddate),executeddate) as time) = p.time_est
					left join keys..pricedatastockbasket sb on sb.symbol = v.currency and cast(v.executeddate as date) = sb.date -- for basket prices at the time of cancellation
					left join keys..pricedatastock ds on ds.ticker = v.currency  
													and ds.date = cast(v.executeddate as date) -- to see if stock price would have gone above cancellation rate
					left join #pricestats news on v.currency = case when sb.symbol is null then news.instrument else news.basket end
												and v.executeddate  >= dateadd(dd,-10,news.publisheddtutc) -- order cancelled up to 10 days before
												and v.executeddate < news.publisheddtutc -- order has to be executed before time of event
					left join #ot ot on ot.server_name = v.server
									and ot.report_id = v.reportid
									and ot.symbol = v.currency
									and v.executeddate>=ot.open_dt -- cancelled orders have to be on opened trades at the time
									and v.executeddate < isnull(ot.cls_dt,getdate())
									and (ot.report_dt=case when cast(v.executeddate as time) <= ''16:59:59'' then convert(nvarchar(10),dateadd(dd,-1,v.executeddate),121) + '' 16:59:59''
															when cast(v.executeddate as time) >= ''16:59:59'' then convert(nvarchar(10),v.executeddate,121)+ '' 16:59:59'' end -- pick the right report_dt based on when the TE occured
										 or (ot.report_dt= convert(nvarchar(10),v.executeddate,121)+ '' 16:59:59'' and ot.type = ''intra'')) --intra day closure
					left join keys..loginacctmapping lam on lam.report_id = v.reportid and lam.server = v.server
														and lam.report_Dt  = ''<yyyymmddt>''
					left join keys..book_entity be on be.book = lam.book_name
					left join keys..cy_<yyyymm_mon> r on r.currency = v.currency
				where v.filter like ''yes''
				and v.exaction like ''delete''
				and v.testflag <> ''y'' 
				and v.accounttype <> ''clearing'' 
				and v.accounttype <> ''40''
				and v.side like ''S'' -- cancelled sell
				and news.sentiment is not null
				and news.sentiment > 0 
				where abs(case when ct.bs like ''B'' 
									then cast (( (( ((p.bid_o - p.bid_hi)/p.bid_hi )*100)) - p.median)/p.mad as decimal (8,3)) 
								when ct.bs like ''S'' 
									then cast (( (( ( (p.ask_o - p.ask_lo)/p.ask_lo )*100)) - p.median)/p.mad as decimal (8,3)) end [z_score]) 
							 > @z_score
				and ot.symbol is null -- only show cancelled orders on positions not opened
				and abs(p.bid_hi-v.rate)<=s.std -- rate at time of cancellation has to be < 1std from the pending order rate
				and v.rate < case when sb.symbol is null then ds.high else sb.high end  -- price of stock has to have gone lower than the cancellation rate for the client to have made profit.
'
set @cpo = replace(@cpo,'<rundt>',convert(nvarchar(10),@rundt,121))
set @cpo = replace(@cpo,'<monthyyyy>',@monthyyyy);
set @cpo=replace(@cpo,'<yyyymmddt>',@yyyymmddt);
set @cpo=replace(@cpo,'<yyyymm_mon>',@yyyymm_mon)
exec (@cpo)







GO


