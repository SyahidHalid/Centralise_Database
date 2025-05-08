declare @acc_total_banking_exposure_fc as decimal(18,2)
--declare @acc_total_banking_exposure_myr as decimal(18,2)
declare @facility_amount_approved as decimal(18,2)
declare @facility_amount_outstanding as decimal(18,2)

declare @facility_exim_account_num as varchar(50) = '330801137107031200'

print @facility_exim_account_num

declare @acc_nature_acc as int
declare @acc_disbursement_status as int
declare @acc_status as int

declare @acc_nature_acc_desc as varchar(50)
declare @acc_disbursement_status_desc as varchar(50)
declare @acc_status_desc as varchar(50)

select @acc_nature_acc=a.acc_nature_acc
, @acc_nature_acc_desc=b.param_name
, @acc_disbursement_status=a.acc_disbursement_status
, @acc_disbursement_status_desc=c.param_name
, @acc_status=a.acc_status
, @acc_status_desc=d.param_name
, @facility_amount_approved=a.facility_amount_approved
, @facility_amount_outstanding=a.facility_amount_outstanding
from col_facilities_application_master a 
left outer join param_system_param b on a.acc_nature_acc = b.param_id 
left outer join param_system_param c on a.acc_disbursement_status = c.param_id 
left outer join param_system_param d on a.acc_status = d.param_id 
where facility_exim_account_num = @facility_exim_account_num

print cast (@acc_nature_acc as varchar) + '-' + @acc_nature_acc_desc
print cast (@acc_disbursement_status as varchar) + '-' + @acc_disbursement_status_desc

--Non Trade -- No Further -- Fully		
if @acc_nature_acc = 30955 		
begin		
	if @acc_disbursement_status = 31531 	
	begin	
		set @acc_total_banking_exposure_fc=@facility_amount_approved
		update col_facilities_application_master set acc_total_banking_exposure_fc=@acc_total_banking_exposure_fc where facility_exim_account_num = @facility_exim_account_num
	end	
end
print @acc_total_banking_exposure_fc		
