from typing import Dict, Any, List, Optional
from config import settings
from src import mongo_setup
from src.utils.json_encoder import serialize_mongo_doc

AVAILABLE_AGGREGATIONS = {
    "sales_by_city": {
        "name": "sales_by_city",
        "description": "Total revenue, order count, and average order value grouped by city.",
        "params": {
            "limit": {"type": "int", "required": False, "default": 20, "description": "Maximum cities to return"}
        }
    },
    "top_customers": {
        "name": "top_customers",
        "description": "Top customers ranked by total spending, order count, and average order value.",
        "params": {
            "limit": {"type": "int", "required": False, "default": 20, "description": "Number of top customers"}
        }
    },
    "orders_by_status": {
        "name": "orders_by_status",
        "description": "Order volume and total financial value categorized by order status.",
        "params": {}
    },
    "sales_by_period": {
        "name": "sales_by_period",
        "description": "Monthly revenue trend and order counts grouped by year-month.",
        "params": {
            "limit": {"type": "int", "required": False, "default": 24, "description": "Number of monthly periods"}
        }
    },
    "payment_and_delivery_breakdown": {
        "name": "payment_and_delivery_breakdown",
        "description": "Cross-analysis of payment methods and delivery types with shipping costs.",
        "params": {
            "limit": {"type": "int", "required": False, "default": 50, "description": "Maximum groups to return"}
        }
    }
}

def get_available_aggregations() -> List[Dict[str, Any]]:
    """Returns metadata for all available aggregation reports."""
    return list(AVAILABLE_AGGREGATIONS.values())

def report_sales_by_city(limit: int = 20, match_filter: Optional[Dict[str, Any]] = None, db=None) -> List[Dict[str, Any]]:
    """Report 1: Revenue, volume, and average order value by city."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
            
        pipeline.extend([
            {
                "$project": {
                    "city": 1,
                    "amount": {"$toDouble": "$total_amount"}
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "total_revenue": {"$sum": "$amount"},
                    "order_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$amount"}
                }
            },
            {"$sort": {"total_revenue": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "order_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]}
                }
            }
        ])
        
        coll = db[settings.COLLECTION_VALIDATED]
        results = list(coll.aggregate(pipeline, allowDiskUse=True))
        return [serialize_mongo_doc(r) for r in results]
    finally:
        if close_client:
            client.close()

def report_top_customers(limit: int = 20, match_filter: Optional[Dict[str, Any]] = None, db=None) -> List[Dict[str, Any]]:
    """Report 2: Top spending customers."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
            
        pipeline.extend([
            {
                "$group": {
                    "_id": "$customer_id",
                    "customer_name": {"$first": "$customer_name"},
                    "total_spent": {"$sum": {"$toDouble": "$total_amount"}},
                    "total_orders": {"$sum": 1}
                }
            },
            {"$sort": {"total_spent": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "customer_id": "$_id",
                    "customer_name": "$customer_name",
                    "total_spent": {"$round": ["$total_spent", 2]},
                    "total_orders": 1,
                    "avg_order_value": {
                        "$round": [
                            {"$cond": [{"$gt": ["$total_orders", 0]}, {"$divide": ["$total_spent", "$total_orders"]}, 0]},
                            2
                        ]
                    }
                }
            }
        ])
        
        coll = db[settings.COLLECTION_VALIDATED]
        results = list(coll.aggregate(pipeline, allowDiskUse=True))
        return [serialize_mongo_doc(r) for r in results]
    finally:
        if close_client:
            client.close()

def report_orders_by_status(match_filter: Optional[Dict[str, Any]] = None, db=None) -> List[Dict[str, Any]]:
    """Report 3: Order counts and financial distribution by status."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
            
        pipeline.extend([
            {
                "$project": {
                    "status": 1,
                    "amount": {"$toDouble": "$total_amount"}
                }
            },
            {
                "$group": {
                    "_id": "$status",
                    "order_count": {"$sum": 1},
                    "total_amount": {"$sum": "$amount"},
                    "avg_amount": {"$avg": "$amount"}
                }
            },
            {"$sort": {"order_count": -1}},
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id",
                    "order_count": 1,
                    "total_amount": {"$round": ["$total_amount", 2]},
                    "avg_amount": {"$round": ["$avg_amount", 2]}
                }
            }
        ])
        
        coll = db[settings.COLLECTION_VALIDATED]
        results = list(coll.aggregate(pipeline, allowDiskUse=True))
        return [serialize_mongo_doc(r) for r in results]
    finally:
        if close_client:
            client.close()

def report_sales_by_period(limit: int = 24, match_filter: Optional[Dict[str, Any]] = None, db=None) -> List[Dict[str, Any]]:
    """Report 4: Monthly sales trends by Year-Month derived from order_date."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
            
        pipeline.extend([
            {
                "$project": {
                    "period": {"$substrCP": ["$order_date", 0, 7]},
                    "amount": {"$toDouble": "$total_amount"}
                }
            },
            {
                "$group": {
                    "_id": "$period",
                    "monthly_revenue": {"$sum": "$amount"},
                    "order_count": {"$sum": 1},
                    "avg_order_value": {"$avg": "$amount"}
                }
            },
            {"$sort": {"_id": 1}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "period": "$_id",
                    "monthly_revenue": {"$round": ["$monthly_revenue", 2]},
                    "order_count": 1,
                    "avg_order_value": {"$round": ["$avg_order_value", 2]}
                }
            }
        ])
        
        coll = db[settings.COLLECTION_VALIDATED]
        results = list(coll.aggregate(pipeline, allowDiskUse=True))
        return [serialize_mongo_doc(r) for r in results]
    finally:
        if close_client:
            client.close()

def report_payment_and_delivery_breakdown(limit: int = 50, match_filter: Optional[Dict[str, Any]] = None, db=None) -> List[Dict[str, Any]]:
    """Report 5: Distribution across payment method and delivery type."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
            
        pipeline.extend([
            {
                "$project": {
                    "payment_method": 1,
                    "delivery_type": 1,
                    "order_amount": {"$toDouble": "$total_amount"},
                    "delivery_amount": {"$toDouble": "$delivery_cost"}
                }
            },
            {
                "$group": {
                    "_id": {
                        "payment_method": "$payment_method",
                        "delivery_type": "$delivery_type"
                    },
                    "order_count": {"$sum": 1},
                    "total_revenue": {"$sum": "$order_amount"},
                    "total_delivery_cost": {"$sum": "$delivery_amount"}
                }
            },
            {"$sort": {"order_count": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "payment_method": "$_id.payment_method",
                    "delivery_type": "$_id.delivery_type",
                    "order_count": 1,
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "total_delivery_cost": {"$round": ["$total_delivery_cost", 2]}
                }
            }
        ])
        
        coll = db[settings.COLLECTION_VALIDATED]
        results = list(coll.aggregate(pipeline, allowDiskUse=True))
        return [serialize_mongo_doc(r) for r in results]
    finally:
        if close_client:
            client.close()

def execute_aggregation(name: str, params: Optional[Dict[str, Any]] = None, db=None) -> Dict[str, Any]:
    """
    Executes an aggregation report by name with given parameters.
    Returns:
        Dict with status, report_name, count, and results.
    """
    if name not in AVAILABLE_AGGREGATIONS:
        raise KeyError(f"Aggregation '{name}' not found. Available reports: {list(AVAILABLE_AGGREGATIONS.keys())}")
        
    params = params or {}
    
    if name == "sales_by_city":
        limit = int(params.get("limit", 20))
        results = report_sales_by_city(limit=limit, db=db)
    elif name == "top_customers":
        limit = int(params.get("limit", 20))
        results = report_top_customers(limit=limit, db=db)
    elif name == "orders_by_status":
        results = report_orders_by_status(db=db)
    elif name == "sales_by_period":
        limit = int(params.get("limit", 24))
        results = report_sales_by_period(limit=limit, db=db)
    elif name == "payment_and_delivery_breakdown":
        limit = int(params.get("limit", 50))
        results = report_payment_and_delivery_breakdown(limit=limit, db=db)
    else:
        results = []
        
    return {
        "status": "success",
        "aggregation_name": name,
        "count": len(results),
        "results": results
    }
