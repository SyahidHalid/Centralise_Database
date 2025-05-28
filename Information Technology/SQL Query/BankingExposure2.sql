--alter PROCEDURE Calculate_Banking_Exposure
CREATE PROCEDURE Calculate_Banking_Exposure
(@facility_exim_account_num as varchar(50))
as
  --exec Calculate_Banking_Exposure '330801137107031200'
  
	declare @acc_total_banking_exposure_fc as decimal(18,2)
	declare @acc_total_banking_exposure_myr as decimal(18,2)
	declare @facility_amount_approved as decimal(18,2)
	declare @facility_amount_approved_myr as decimal(18,2)
	declare @facility_amount_outstanding as decimal(18,2)
	declare @acc_principal_amount_outstanding as decimal(18,2)

	--declare @facility_exim_account_num as varchar(50) = '330801137107031200'

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
	, @facility_amount_approved_myr=a.facility_amount_approved_myr
	, @facility_amount_outstanding=a.facility_amount_outstanding
	, @acc_principal_amount_outstanding=a.acc_principal_amount_outstanding
	from col_facilities_application_master a 
	left outer join param_system_param b on a.acc_nature_acc = b.param_id 
	left outer join param_system_param c on a.acc_disbursement_status = c.param_id 
	left outer join param_system_param d on a.acc_status = d.param_id 
	where facility_exim_account_num = @facility_exim_account_num
BEGIN
    SET NOCOUNT ON;

	-- Active  -- Non Trade (-- No Further -- Fully)
    IF @acc_status IN (30947,30948,30949) AND @acc_nature_acc = 30955 AND (@acc_disbursement_status = 30967 OR @acc_disbursement_status = 30968)
    BEGIN
        SET @acc_total_banking_exposure_fc = @facility_amount_outstanding
		SET @acc_total_banking_exposure_myr = @acc_principal_amount_outstanding
    END

	-- Pending -- Non Trade -- Ongoing -- 
    ELSE IF @acc_status IN (30946,30947,30948,30949) AND @acc_nature_acc = 30955 AND @acc_disbursement_status = 31531
    BEGIN
        SET @acc_total_banking_exposure_fc = @facility_amount_approved
		SET @acc_total_banking_exposure_myr = @facility_amount_approved_myr
    END

	-- Active  -- Trade (-- No Further -- Fully)
    ELSE IF @acc_status IN (30947,30948,30949) AND @acc_nature_acc IN (30956,31697) AND (@acc_disbursement_status = 30967 OR @acc_disbursement_status = 30968)
    BEGIN
        SET @acc_total_banking_exposure_fc = @facility_amount_outstanding
		SET @acc_total_banking_exposure_myr = @acc_principal_amount_outstanding
    END

	-- Pending -- Trade -- Ongoing -- 
    ELSE IF @acc_status IN (30946,30947,30948,30949) AND @acc_nature_acc IN (30956,31697) AND @acc_disbursement_status = 31531
    BEGIN
        SET @acc_total_banking_exposure_fc = @facility_amount_approved
		SET @acc_total_banking_exposure_myr = @facility_amount_approved_myr
    END

	-- Impaired
    ELSE IF @acc_status = 30952
    BEGIN
        SET @acc_total_banking_exposure_fc = @facility_amount_outstanding
		SET @acc_total_banking_exposure_myr = @acc_principal_amount_outstanding
    END

	-- 0
    ELSE
    BEGIN
        SET @acc_total_banking_exposure_fc = 0
		SET @acc_total_banking_exposure_myr = 0
    END
	print @acc_total_banking_exposure_fc
	update col_facilities_application_master set acc_total_banking_exposure_fc=@acc_total_banking_exposure_fc where facility_exim_account_num = @facility_exim_account_num
END