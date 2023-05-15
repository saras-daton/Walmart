{% if var('WalmartReconciliationReport') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}


{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}


{% set table_name_query %}
{{set_table_name('%walmart%reconciliationreport')}}    
{% endset %}


{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * {{exclude()}}(row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        ReportDate,
        Walmart_dot_com_Order__hash,
        Walmart_dot_com_Order_Line__hash,
        Walmart_dot_com_PO__hash,
        Walmart_dot_com_P_dot_O_dot__Line__hash,
        Partner_Order__hash,
        Transaction_Type,
        Transaction_Date_Time,
        Shipped_Qty,
        Partner_Item_ID,
        Partner_GTIN,
        Partner_Item_name,
        Product_tax_code,
        Shipping_tax_code,
        Gift_wrap_tax_code,
        Ship_to_state,
        Ship_to_county,
        County_Code,
        Ship_to_city,
        Zip_code,
        shipping_Underscore_method,
        Total_tender_to__slash__from_customer,
        Payable_to_Partner_from_Sale,
        Commission_from_Sale,
        Commission_Rate,
        Gross_Sales_Revenue,
        Refunded_Retail_Sales,
        Sales_refund_for_Escalation,
        Gross_Shipping_Revenue,
        Gross_Shipping_Refunded,
        Shipping_refund_for_Escalation,
        Net_Shipping_Revenue,
        Gross_Fee_Revenue,
        Gross_Fee_Refunded,
        Fee_refund_for_Escalation,
        Net_Fee_Revenue,
        Gift_Wrap_Quantity,
        Gross_Gift_Dash_Wrap_Revenue,
        Gross_Gift_Dash_Wrap_Refunded,
        Gift_wrap_refund_for_Escalation,
        Net_Gift_Wrap_Revenue,
        Tax_on_Sales_Revenue,
        Tax_on_Shipping_Revenue,
        Tax_on_Gift_Dash_Wrap_Revenue,
        Tax_on_Fee_Revenue,
        Effective_tax_rate,
        Tax_on_Refunded_Sales,
        Tax_on_Shipping_Refund,
        Tax_on_Gift_Dash_Wrap_Refund,
        Tax_on_Fee_Refund,
        Tax_on_Sales_refund_for_Escalation,
        Tax_on_Shipping_Refund_for_Escalation,
        Tax_on_Gift_Dash_Wrap_Refund_for_escalation,
        Tax_on_Fee_Refund_for_escalation,
        Total_NET_Tax_Collected,
        Tax_Withheld,
        Adjustment_Description,
        Adjustment_Code,
        Original_Item_price,
        Original_Commission_Amount,
        Product_Type,
        Spec_Category,
        Contract_Category,
        Flex_Commission_Rule,
        Return_Reason_Code,
        Return_Reason_Description,
        Fee_Withheld_Flag,
        Fulfillment_Type,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY Walmart_dot_com_PO__hash, Walmart_dot_com_P_dot_O_dot__Line__hash,Transaction_Type,Adjustment_Code order by a.{{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
            )
        where row_num = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}




