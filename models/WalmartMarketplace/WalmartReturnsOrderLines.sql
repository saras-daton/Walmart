{% if var('WalmartReturnsOrderLines') %}
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

with Returns as(
{% set table_name_query %}
{{set_table_name('%walmart%returns')}}    
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

    SELECT * 
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.totalRefundAmount_currencyUnit else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            cast(null as string) as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        returnOrderId,
        customerEmailId,
        {% if target.type=='snowflake' %} 
        customerName.VALUE:firstName::VARCHAR as firstName,
        customerName.VALUE:lastName::VARCHAR as lastName,
        customerOrderId,
        CAST(returnOrderDate as DATE) returnOrderDate,
        CAST(returnByDate as DATE) returnByDate,
        refundMode,
        totalRefundAmount.VALUE:currencyAmount::FLOAT as totalRefundAmount_currencyAmount,
        totalRefundAmount.VALUE:currencyUnit::VARCHAR as totalRefundAmount_currencyUnit,
        returnLineGroups,
        returnOrderLines.VALUE:returnOrderLineNumber::VARCHAR as returnOrderLineNumber,
        returnOrderLines.VALUE:salesOrderLineNumber::VARCHAR as salesOrderLineNumber,
        returnOrderLines.VALUE:returnReason::VARCHAR as returnReason,
        returnOrderLines.VALUE:purchaseOrderId::VARCHAR as purchaseOrderId,
        returnOrderLines.VALUE:purchaseOrderLineNumber::VARCHAR as purchaseOrderLineNumber,
        returnOrderLines.VALUE:isReturnForException::VARCHAR as isReturnForException,
        item.VALUE:sku::VARCHAR as sku,
        item.VALUE:productName::VARCHAR as productName,
        itemWeight.VALUE:unitOfMeasure::VARCHAR as itemWeight_unitOfMeasure,
        itemWeight.VALUE:measurementValue::FLOAT as itemWeight_measurementValue,
        charges.VALUE:chargeCategory::VARCHAR as chargeCategory,
        charges.VALUE:chargeName::VARCHAR as chargeName,
        chargePerUnit.VALUE:currencyAmount::FLOAT as chargePerUnit_currencyAmount,
        chargePerUnit.VALUE:currencyUnit::VARCHAR as chargePerUnit_currencyUnit,
        charges.VALUE:isDiscount::VARCHAR as isDiscount,
        charges.VALUE:isBillable::VARCHAR as isBillable,
        --charges.VALUE:tax::FLOAT as tax,
        --tax.VALUE:tax::FLOAT as tax,
        --charges.VALUE:excessCharge::FLOAT as excessCharge,
        charges.VALUE:references::VARCHAR as charges_references,
        excessTax.VALUE:currencyAmount::FLOAT as excessTax_currencyAmount,
        excessTax.VALUE:currencyUnit::VARCHAR as excessTax_currencyUnit,
        tax.VALUE:taxName::VARCHAR as taxName,
        taxPerUnit.VALUE:currencyAmount::FLOAT as taxPerUnit_currencyAmount,
        taxPerUnit.VALUE:currencyUnit::VARCHAR as taxPerUnit_currencyUnit,
        unitPrice.VALUE:currencyAmount::FLOAT as unitPrice_currencyAmount,
        unitPrice.VALUE:currencyUnit::VARCHAR as unitPrice_currencyUnit,
        returnOrderLines.VALUE:itemReturnSettings::VARCHAR as itemReturnSettings,
        chargeTotals.VALUE:name::VARCHAR as chargeTotals_name,
        value.VALUE:currencyAmount::FLOAT as chargeTotals_value_currencyAmount,
        value.VALUE:currencyUnit::VARCHAR as chargeTotals_value_currencyUnit,
        returnOrderLines.VALUE:cancellableQty::VARCHAR as cancellableQty,
        quantity.VALUE:unitOfMeasure::VARCHAR as quantity_unitOfMeasure,
        quantity.VALUE:measurementValue::FLOAT as quantity_measurementValue,
        returnOrderLines.VALUE:returnExpectedFlag::VARCHAR as returnExpectedFlag,
        returnOrderLines.VALUE:isFastReplacement::VARCHAR as isFastReplacement,
        returnOrderLines.VALUE:isKeepIt::VARCHAR as isKeepIt,
        returnOrderLines.VALUE:lastItem::VARCHAR as lastItem,
        returnOrderLines.VALUE:refundedQty::NUMERIC as refundedQty,
        returnOrderLines.VALUE:rechargeableQty::VARCHAR as rechargeableQty,
        returnOrderLines.VALUE:refundChannel::VARCHAR as refundChannel,
        returnOrderLines.VALUE:returnTrackingDetail,
        returnOrderLines.VALUE:status::VARCHAR as status,
        returnOrderLines.VALUE:statusTime::timestamp as statusTime,
        returnOrderLines.VALUE:currentDeliveryStatus::VARCHAR as currentDeliveryStatus,
        returnOrderLines.VALUE:currentRefundStatus::VARCHAR as currentRefundStatus,
        returnChannel.VALUE:channelName::VARCHAR as channelName,
        {% else %}
        customerName.firstName,
        customerName.lastName,
        customerOrderId,
        CAST(returnOrderDate as DATE) returnOrderDate,
        CAST(returnByDate as DATE) returnByDate,
        refundMode,
        totalRefundAmount.currencyAmount as totalRefundAmount_currencyAmount,
        totalRefundAmount.currencyUnit as totalRefundAmount_currencyUnit,
        returnLineGroups,
        returnOrderLines.returnOrderLineNumber,
        returnOrderLines.salesOrderLineNumber,
        returnOrderLines.returnReason,
        returnOrderLines.purchaseOrderId,
        returnOrderLines.purchaseOrderLineNumber,
        returnOrderLines.isReturnForException,
        item.sku,
        item.productName,
        itemWeight.unitOfMeasure as itemWeight_unitOfMeasure,
        itemWeight.measurementValue as itemWeight_measurementValue,
        charges.chargeCategory,
        charges.chargeName,
        chargePerUnit.currencyAmount as chargePerUnit_currencyAmount,
        chargePerUnit.currencyUnit as chargePerUnit_currencyUnit,
        charges.isDiscount,
        charges.isBillable,
        charges.tax,
        charges.excessCharge,
        charges.references as charges_references,
        unitPrice.currencyAmount as unitPrice_currencyAmount,
        unitPrice.currencyUnit as unitPrice_currencyUnit,
        returnOrderLines.itemReturnSettings,
        chargeTotals.name as chargeTotals_name,
        value.currencyAmount as chargeTotals_value_currencyAmount,
        value.currencyUnit as chargeTotals_value_currencyUnit,
        returnOrderLines.cancellableQty,
        quantity.unitOfMeasure as quantity_unitOfMeasure,
        quantity.measurementValue as quantity_measurementValue,
        returnOrderLines.returnExpectedFlag,
        returnOrderLines.isFastReplacement,
        returnOrderLines.isKeepIt,
        returnOrderLines.lastItem,
        returnOrderLines.refundedQty,
        returnOrderLines.rechargeableQty,
        returnOrderLines.refundChannel,
        returnOrderLines.returnTrackingDetail,
        returnOrderLines.status,
        returnOrderLines.statusTime,
        returnOrderLines.currentDeliveryStatus,
        returnOrderLines.currentRefundStatus,
        returnChannel.channelName,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        FROM  {{i}} 
            {{unnesting("CUSTOMERNAME")}}
            {{unnesting("TOTALREFUNDAMOUNT")}}
            {{unnesting("RETURNORDERLINES")}}
            {{unnesting("RETURNCHANNEL")}}
            {{multi_unnesting("RETURNORDERLINES","item")}}
            {{multi_unnesting("RETURNORDERLINES","charges")}}
            {{multi_unnesting("RETURNORDERLINES","unitPrice")}}
            {{multi_unnesting("RETURNORDERLINES","chargeTotals")}}
            {{multi_unnesting("RETURNORDERLINES","quantity")}}
            {{multi_unnesting("item","itemWeight")}}
            {{multi_unnesting("charges","chargePerUnit")}}
            {{multi_unnesting("charges","tax")}}
            {{multi_unnesting("tax","excessTax")}}
            {{multi_unnesting("tax","taxPerUnit")}}
            {{multi_unnesting("chargeTotals","value")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        ) a
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(returnOrderDate) = c.date and a.totalRefundAmount_currencyUnit = c.to_currency_code
        {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,   
ROW_NUMBER() OVER (PARTITION BY returnOrderId, customerOrderId, chargeTotals_name order by _daton_batch_runtime desc) row_num
from Returns
)

select * {{exclude()}}(row_num)
from dedup 
where row_num = 1



