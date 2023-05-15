{% if var('WalmartOrders') %}
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

with Orders as(
{% set table_name_query %}
{{set_table_name('%walmart%orders')}}    
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
            case when c.from_currency_code is null then a.chargeAmount_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.chargeAmount_currency as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        statusCodeFilter,
        {% if target.type=='snowflake' %} 
        shipNodeType,
        {% else %}
        b.shipNodeType,
        {% endif %}
        purchaseOrderId,
        customerOrderId,
        customerEmailId,
        orderType,
        cast(orderDate as DATE) orderDate,
        {% if target.type=='snowflake' %} 
        shippingInfo.VALUE:phone::VARCHAR as phone,
        shippingInfo.VALUE:estimatedDeliveryDate::DATE as estimatedDeliveryDate,
        shippingInfo.VALUE:estimatedShipDate::DATE as estimatedShipDate,
        shippingInfo.VALUE:methodCode::VARCHAR as methodCode,
        postalAddress.VALUE:name::VARCHAR as full_name,
        postalAddress.VALUE:address1::VARCHAR as ship_address1,
        postalAddress.VALUE:address2::VARCHAR as ship_address2,
        postalAddress.VALUE:address3::VARCHAR as ship_address3,
        postalAddress.VALUE:address4::VARCHAR as ship_address4,
        postalAddress.VALUE:city::VARCHAR as ship_city,
        postalAddress.VALUE:state::VARCHAR as ship_state,
        postalAddress.VALUE:postalCode::VARCHAR as ship_postal_code,
        postalAddress.VALUE:country::VARCHAR as ship_country,
        billingInfo,
        totalLines,
        totalQuantity,
        orderLine.VALUE:lineNumber::VARCHAR as lineNumber,
        orderLine.VALUE:primeLineNumber::VARCHAR as primeLineNumber,
        orderLine.VALUE:coLineNumber::VARCHAR as coLineNumber,
        item.VALUE:upc::VARCHAR as upc,
        item.VALUE:offerId::VARCHAR as offerId,
        item.VALUE:productName::VARCHAR as productName,
        item.VALUE:sku::VARCHAR as sku,
        item.VALUE:condition::VARCHAR as condition,
        item.VALUE:imageUrl::VARCHAR as imageUrl,
        unitPrice.VALUE:currency::VARCHAR as unitPrice_currency,
        unitPrice.VALUE:amount::FLOAT as unitPrice_amount,
        commission.VALUE:currency::VARCHAR as commission_currency,
        commission.VALUE:amount::FLOAT as commission_amount,
        unitPriceWithoutTax.VALUE:currency::VARCHAR as unitPriceWithoutTax_currency,
        unitPriceWithoutTax.VALUE:amount::FLOAT as unitPriceWithoutTax_amount,
        charge.VALUE:chargeType::VARCHAR as chargeType,
        charge.VALUE:chargeName::VARCHAR as chargeName,
        chargeAmount.VALUE:currency::VARCHAR as chargeAmount_currency,
        chargeAmount.VALUE:amount::FLOAT as chargeAmount_amount,
        charge.VALUE:tax::VARCHAR as tax,
        tax.VALUE:taxName::VARCHAR as taxName,
        taxAmount.VALUE:currency::VARCHAR as tax_currency,
        taxAmount.VALUE:amount::FLOAT as tax_amount,
        orderLineQuantity.VALUE:unitOfMeasurement::VARCHAR as orderLineQuantity_unitOfMeasurement,
        orderLineQuantity.VALUE:amount::FLOAT as orderLineQuantity_amount,
        orderLine.VALUE:statusDate::DATE as statusDate,
        orderLineStatuses.VALUE:orderLineStatus::VARCHAR as orderLineStatus,
        orderLine.VALUE:shippingMethod::VARCHAR as shippingMethod,
        orderLine.VALUE:soPrimeLineSubLineNo::VARCHAR as soPrimeLineSubLineNo,
        orderLine.VALUE:promiseDeliveryDate::timestamp as promiseDeliveryDate,
        orderLine.VALUE:isMSIEnabled::VARCHAR as isMSIEnabled,
        orderLine.VALUE:refund::VARCHAR as refund,
        seller.VALUE:id::VARCHAR as seller_id,
        seller.VALUE:name::VARCHAR as seller_name,
        seller.VALUE:shipNodeType::VARCHAR as seller_shipNodeType,
        fulfillment.VALUE:fulfillmentOption::VARCHAR as fulfillmentOption,
        fulfillment.VALUE:shipMethod::VARCHAR as shipMethod,
        fulfillment.VALUE:pickUpDateTime::timestamp as pickUpDateTime,
        fulfillment.VALUE:shippingProgramType::VARCHAR as shippingProgramType,
        shipNode,
        shipmentLines.VALUE:primeLineNo::VARCHAR as primeLineNo,
        shipmentLines.VALUE:shipmentLineNo::VARCHAR as shipmentLineNo,
        shipmentLines.VALUE:quantity::NUMERIC as shipment_quantity,
        shipments.VALUE:shipmentNo::VARCHAR as shipmentNo,
        shipments.VALUE:status::VARCHAR as status,
        shipments.VALUE:packageNo::VARCHAR as packageNo,
        shipments.VALUE:isSellerOwnedShipment::VARCHAR as isSellerOwnedShipment,
        shipments.VALUE:carrier::VARCHAR as carrier,
        shipments.VALUE:trackingNumber::VARCHAR as trackingNumber,
        shipments.VALUE:trackingUrl::VARCHAR as trackingUrl,
        shipments.VALUE:shipmentAdditionalDate::VARCHAR as shipmentAdditionalDate,
        orderTotal.VALUE:currency::VARCHAR as orderTotal_currency,
        orderTotal.VALUE:amount::FLOAT as orderTotal_amount,
        {% else %}
        shippingInfo.phone,
        shippingInfo.estimatedDeliveryDate,
        shippingInfo.estimatedShipDate,
        shippingInfo.methodCode,
        postalAddress.name as full_name,
        postalAddress.address1 as ship_address1,
        postalAddress.address2 as ship_address2,
        postalAddress.address3 as ship_address3,
        postalAddress.address4 as ship_address4,
        postalAddress.city as ship_city,
        postalAddress.state as ship_state,
        postalAddress.postalCode as ship_postal_code,
        postalAddress.country as ship_country,
        billingInfo,
        totalLines,
        totalQuantity,
        orderLine.lineNumber,
        orderLine.primeLineNumber,
        orderLine.coLineNumber,
        item.upc,
        item.offerId,
        item.productName,
        item.sku,
        item.condition,
        item.imageUrl,
        unitPrice.currency as unitPrice_currency,
        unitPrice.amount as unitPrice_amount,
        commission.currency as commission_currency,
        commission.amount as commission_amount,
        unitPriceWithoutTax.currency as unitPriceWithoutTax_currency,
        unitPriceWithoutTax.amount as unitPriceWithoutTax_amount,
        charge.chargeType,
        charge.chargeName,
        chargeAmount.currency as chargeAmount_currency,
        chargeAmount.amount as chargeAmount_amount,
        charge.tax,
        tax.taxName,
        taxAmount.currency as tax_currency,
        taxAmount.amount as tax_amount,
        orderLineQuantity.unitOfMeasurement as orderLineQuantity_unitOfMeasurement,
        orderLineQuantity.amount as orderLineQuantity_amount,
        orderLine.statusDate,
        orderLineStatuses.orderLineStatus,
        orderLine.shippingMethod,
        orderLine.soPrimeLineSubLineNo,
        orderLine.promiseDeliveryDate,
        orderLine.isMSIEnabled,
        orderLine.refund,
        seller.id as seller_id,
        seller.name as seller_name,
        seller.shipNodeType as seller_shipNodeType,
        fulfillment.fulfillmentOption,
        fulfillment.shipMethod,
        fulfillment.pickUpDateTime,
        fulfillment.shippingProgramType,
        shipNode,
        shipmentLines.primeLineNo,
        shipmentLines.shipmentLineNo,
        shipmentLines.quantity as shipment_quantity,
        shipments.shipmentNo,
        shipments.status,
        shipments.packageNo,
        shipments.isSellerOwnedShipment,
        shipments.carrier,
        shipments.trackingNumber,
        shipments.trackingUrl,
        shipments.shipmentAdditionalDate,
        orderTotal.currency as orderTotal_currency,
        orderTotal.amount as orderTotal_amount,
        {% endif %}
        rfc,
        paymentMethod,
        cfdi,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        FROM  {{i}} b
            {{unnesting("ORDERLINES")}}
            {{unnesting("SHIPPINGINFO")}}
            {{unnesting("SHIPMENTS")}}
            {{multi_unnesting("ORDERLINES","orderLine")}}
            {{unnesting("ORDERTOTAL")}}
            {{multi_unnesting("SHIPPINGINFO","postalAddress")}}
            {{multi_unnesting("SHIPMENTS","shipmentLines")}}
            {{multi_unnesting("orderLine","item")}}
            {{multi_unnesting("orderLine","charges")}}
            {{multi_unnesting("orderLine","orderLineQuantity")}}
            {{multi_unnesting("orderLine","seller")}}
            {{multi_unnesting("orderLine","fulfillment")}}
            {{multi_unnesting("orderLine","orderLineStatuses")}}
            {{multi_unnesting("item","unitPrice")}}
            {{multi_unnesting("item","commission")}}
            {{multi_unnesting("item","unitPriceWithoutTax")}}
            {{multi_unnesting("charges","charge")}}
            {{multi_unnesting("charge","chargeAmount")}}
            {{multi_unnesting("charge","tax")}}
            {{multi_unnesting("tax","taxAmount")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        ) a
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.orderDate) = c.date and a.chargeAmount_currency = c.to_currency_code
        {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY purchaseOrderId, customerOrderId, lineNumber, chargeType, sku, orderDate,productName order by _daton_batch_runtime desc) row_num
from Orders
)

select * {{exclude()}}(row_num)
from dedup 
where row_num = 1

