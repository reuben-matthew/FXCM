
declare @monthyyyy varchar(15),@yyyymm_mon varchar(10),@year varchar(4), @yyyymmdd varchar(10), @yyyymmdd2 varchar(10), @yyyymmdd_p varchar(10),
			@counter_mon int,@counter_ttl int,@sql varchar(max),@sql2 varchar(max),@sql3 varchar(max)
	set @sql = ''
	set @sql2 = 'select ''<monthyyyy>'' period
					,v.Book
					,v."Server"
					,v.reportid
					,cl.correctcountry
					,v.country
					,v.currency
				   ,sum(abs(v.originqty)) executed_volume
				   ,sum(abs(v.originqty)*r.rate)notional_volume
				   ,count(*) as number_of_trades
				 from volume.dbo.<monthyyyy>volume v
							 left join keys.dbo.cy_<yyyymm_mon> r on v.currency = r.currency
							 left join keys.dbo.booklist b on v.book = b.book
							 left join keys.dbo.countrylist cl on v.country = cl.country
				 where  v.filter like ''yes''
				group by v.Book,v."Server",v.reportid,cl.correctcountry,v.country,v.currency' 
	set @year = 2014
	set @counter_mon = 12
	set @counter_ttl = 11
	while @counter_ttl > 0 begin
		while @counter_mon <= 12 and @counter_ttl > 0 begin
			set @yyyymm_mon = 
				(select @year + right('0' + cast(@counter_mon as varchar(2)), 2) + '_' 
							  + left(datename(mm,cast(@counter_mon as varchar(2))+'/1/1900'),3))
			set @monthyyyy = (select datename(mm,cast(@counter_mon as varchar(2))+'/1/1900')+@year)
			set @yyyymmdd = cast(@year as varchar(4)) + '-' + right('0'+cast(@counter_mon as varchar(2)),2) + '-01' 
			set @sql3 = @sql2
			set @sql3 = replace(@sql3,'<monthyyyy>',@monthyyyy);
			set @sql3 = replace(@sql3,'<yyyymm_mon>',@yyyymm_mon);
			set @sql3 = replace(@sql3,'<mm>',right('0'+cast(@counter_mon as nvarchar(2)),2));
			set @sql3 = replace(@sql3,'<yyyy>',@year);
			set @sql3 = replace(@sql3,'<mm_next>',case when @counter_mon = 12 then '01' else right('0'+cast(@counter_mon+1 as nvarchar(2)),2) end);
			set @sql3 = replace(@sql3,'<yyyymmdd>',@yyyymmdd);
			set @sql = @sql + @sql3				
			set @counter_mon = @counter_mon + 1 
			set	@counter_ttl = @counter_ttl - 1 	
			if @counter_ttl <> 0 set @sql = @sql + ' union all '
		end set @counter_mon = 1 set @year = @year + 1
	end exec(@sql)

