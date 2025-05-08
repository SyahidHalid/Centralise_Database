exec Calculate_Banking_Exposure '330801137107031200'

select a.acc_status,b.param_name,a.acc_nature_acc,c.param_name,a.acc_disbursement_status,d.param_name,a.facility_amount_approved,a.facility_amount_outstanding,a.acc_total_banking_exposure_fc from col_facilities_application_master a
left outer join param_system_param b on a.acc_status = b.param_id 
left outer join param_system_param c on a.acc_nature_acc = c.param_id 
left outer join param_system_param d on a.acc_disbursement_status = d.param_id 
where facility_exim_account_num = '330801137107031200'