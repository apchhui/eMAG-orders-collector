const { DateTime } = require('luxon');
const fetch = require('node-fetch');
const fs = require('fs');
const YAML = require('yaml');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

function convertToBoolean(value) {
    if (['1', 1, true, 'true'].includes(value)) return true;
    if (['0', 0, false, 'false'].includes(value)) return false;
    return null;
}

function loadConfig() {
    const configPath = 'config.yaml';
    if (!fs.existsSync(configPath)) throw new Error(`Config file ${configPath} not found.`);
    const config = YAML.parse(fs.readFileSync(configPath, 'utf8'));
    return config;
}

function parseYAML(config) {
    const { supabase_url, supabase_key } = config.supabase;
    const { username, password, emag_url } = config;

    const authStr = `${username}:${password}`;
    const auth = Buffer.from(authStr, 'utf-8').toString('base64');

    return {
        supabase: createClient(supabase_url, supabase_key),
        auth,
        emag_url
    };
}

const config = loadConfig();
const { supabase, auth, emag_url } = parseYAML(config);

async function loadExistingOrderIds() {
    const { data: orders, error } = await supabase
        .from('orders')
        .select('order_id');
    if (error) throw error;
    return new Set(orders.map(order => order.order_id));
}

async function saveOrderToDb(order) {
    try {
        const orderData = {
            order_id: order.id,
            vendor_name: order.vendor_name,
            type: order.type,
            parent_id: order.parent_id,
            date: order.date,
            payment_mode: order.payment_mode,
            detailed_payment_method: order.detailed_payment_method,
            delivery_mode: order.delivery_mode,
            observation: order.observation,
            modified: order.modified,
            status: order.status,
            payment_status: order.payment_status,
            customer_details: JSON.stringify(order.customer || {}),
            shipping_tax: order.shipping_tax,
            cashed_co: order.cashed_co,
            cashed_cod: order.cashed_cod,
            cancellation_request: convertToBoolean(order.cancellation_request),
            has_editable_products: convertToBoolean(order.has_editable_products),
            refunded_amount: order.refunded_amount,
            is_complete: convertToBoolean(order.is_complete),
            reason_cancellation: order.reason_cancellation,
            refund_status: convertToBoolean(order.refund_status),
            maximum_date_for_shipment: order.maximum_date_for_shipment,
            late_shipment: convertToBoolean(order.late_shipment),
            emag_club: convertToBoolean(order.emag_club),
            finalization_date: order.finalization_date,
            enforced_vendor_courier_accounts: convertToBoolean(order.enforced_vendor_courier_accounts),
            weekend_delivery: convertToBoolean(order.weekend_delivery),
            payment_mode_id: order.payment_mode_id,
            flags: JSON.stringify(order.flags || {}),
            details: JSON.stringify(order.details || {}),
            attachments: JSON.stringify(order.attachments || {})
        };

        const { error: orderError } = await supabase
            .from('orders')
            .upsert([orderData], { onConflict: 'order_id' });

        if (orderError) throw orderError;

        const productsData = (order.products || []).map(product => ({
            product_id: product.id,
            order_id: order.id,
            name: product.name,
            ext_part_number: product.ext_part_number,
            part_number_key: product.part_number_key,
            sale_price: product.sale_price,
            quantity: product.quantity,
            original_price: product.original_price,
            currency: product.currency,
            created: product.created,
            modified: product.modified,
            retained_amount: product.retained_amount,
            vat: product.vat,
            status: product.status,
            part_number: product.part_number,
            mkt_id: product.mkt_id,
            initial_qty: product.initial_qty,
            storno_qty: product.storno_qty,
            reversible_vat_charging: convertToBoolean(product.reversible_vat_charging),
            recycle_warranties: JSON.stringify(product.recycle_warranties || []),
            product_voucher_split: JSON.stringify(product.product_voucher_split || []),
            details: JSON.stringify(product.details || {}),
            attachments: JSON.stringify(product.attachments || {}),
            cost: (parseFloat(product.sale_price) || 0) * (parseFloat(product.quantity) || 0)
        }));

        const { error: productsError } = await supabase
            .from('products')
            .upsert(productsData, { onConflict: 'product_id' });

        if (productsError) throw productsError;

        console.log(`Order ${order.id} saved successfully.`);
    } catch (error) {
        console.error(`Error saving order ${order.id}:`, error);
    }
}
async function fetchAllOrders() {
    const existingIds = await loadExistingOrderIds();
    const startDate = DateTime.local(2024, 12, 10);
    const endDate = DateTime.local();

    let currentStart = startDate;

    while (currentStart < endDate) {
        const nextMonth = currentStart.plus({ months: 1 });
        const dateFrom = currentStart.toISO();
        const dateTo = nextMonth.toISO();

        console.log(`Fetching range [${dateFrom}..${dateTo}]`);
        await fetchDateRangeWithPagingAndSplit(dateFrom, dateTo, existingIds, 0);

        currentStart = nextMonth;
    }
}

async function fetchDateRangeWithPagingAndSplit(dateFrom, dateTo, existingIds, depth) {
    const MAX_DEPTH = 10;
    if (depth > MAX_DEPTH) return;

    const res = await fetchOneRangePaged(dateFrom, dateTo, existingIds);
    if (res.gotStuck) {
        const midDate = DateTime.fromISO(dateFrom)
            .plus({ days: Math.floor(DateTime.fromISO(dateTo).diff(DateTime.fromISO(dateFrom), 'days').days / 2) });
        await fetchDateRangeWithPagingAndSplit(dateFrom, midDate.toISO(), existingIds, depth + 1);
        await fetchDateRangeWithPagingAndSplit(midDate.toISO(), dateTo, existingIds, depth + 1);
    }
}

async function fetchOneRangePaged(dateFrom, dateTo, existingIds) {
    let currentPage = 1;
    let consecutiveEmptyEntries = 0;

    while (true) {
        const orders = await callEmagOrderRead(currentPage, dateFrom, dateTo);
        console.log(`Range [${dateFrom}..${dateTo}], page=${currentPage}, got ${orders.length} orders`);

        if (orders.length === 0) break;

        let newCount = 0;
        for (const order of orders) {
            if (!existingIds.has(order.id)) {
                await saveOrderToDb(order);
                existingIds.add(order.id);
                newCount++;
            }
        }

        if (orders.length < 100) break;

        if (orders.length === 100 && newCount === 0) {
            consecutiveEmptyEntries++;
            if (consecutiveEmptyEntries >= 3) return { gotStuck: true };
        } else {
            consecutiveEmptyEntries = 0;
        }

        currentPage++;
    }

    return { gotStuck: false };
}

async function callEmagOrderRead(currentPage, dateFrom, dateTo) {
    const payload = {
        currentPage,
        itemsPerPage: 100,
        data: {
            createdAfter: dateFrom,
            createdBefore: dateTo,
            status: [1, 2, 3, 4]
        }
    };

    const headers = {
        Authorization: `Basic ${auth}`,
        'Content-Type': 'application/json'
    };

    try {
        const response = await fetch(emag_url, {
            method: 'POST',
            headers,
            body: JSON.stringify(payload)
        });

        const data = await response.json();
        if (data.isError) {
            console.error(`eMAG returned error: ${JSON.stringify(data.messages || [])}`);
            return [];
        }
        return data.results || [];
    } catch (error) {
        console.error('API request failed:', error);
        return [];
    }
}

async function readAndProcessOrders() {
    try {
        const { data: orders, error: ordersError } = await supabase.from('orders').select('*');
        if (ordersError) throw ordersError;
      
        const { data: products, error: productsError } = await supabase.from('products').select('*');
        if (productsError) throw productsError;
      
        const productsByOrderId = {};
        products.forEach(product => {
            if (!productsByOrderId[product.order_id]) {
                productsByOrderId[product.order_id] = [];
            }
            productsByOrderId[product.order_id].push(product);
        });

        console.log('=== Orders Summary ===');
        orders.forEach(order => {
            console.log(`\nOrder ID: ${order.order_id}`);
            console.log(`Vendor Name: ${order.vendor_name}`);
            console.log(`Date: ${order.date}`);
            console.log(`Status: ${order.status}`);
            console.log(`Payment Status: ${order.payment_status}`);
            console.log(`Total Products: ${productsByOrderId[order.order_id]?.length || 0}`);

            if (productsByOrderId[order.order_id]) {
                console.log('--- Products ---');
                productsByOrderId[order.order_id].forEach((product, index) => {
                    console.log(`Product ${index + 1}:`);
                    console.log(`  Product ID: ${product.product_id}`);
                    console.log(`  Name: ${product.name}`);
                    console.log(`  Quantity: ${product.quantity}`);
                    console.log(`  Price: ${product.sale_price}`);
                    console.log(`  Status: ${product.status}`);
                });
            } else {
                console.log('No products found for this order.');
            }
        });
    } catch (error) {
        console.error('Error reading and processing orders:', error);
    }
}

fetchAllOrders()
    .then(() => {
        console.log('Data fetching completed');
        readAndProcessOrders();
    })
    .catch(err => console.error('Error:', err));⏎  
