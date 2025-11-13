#!/usr/bin/env python3
"""
Airline Booking FastMCP Server (PostgreSQL - No Pre-Registration Required)

Works with PostgreSQL database - no pre-registration needed!
Just provide passenger info when booking.
"""

import psycopg2
import psycopg2.extras
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
from contextlib import contextmanager
import random
import os

from fastmcp import FastMCP

from dotenv import load_dotenv
load_dotenv() # This loads variables from the .env file


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airline-booking-mcp")

# Database configuration
DB_URL = os.getenv("DATABASE_URL")

# Initialize FastMCP server
mcp = FastMCP(
    name="airline-booking-mcp",
    instructions="""
    This server provides airline booking capabilities with international coverage.
    
    No pre-registration required! Just provide:
    - Passenger name and email
    - Corporate status (if applicable)
    - Company name (if corporate)
    
    Supports 20+ airlines and 27+ airports worldwide with 100+ routes.
    Corporate discounts are applied automatically based on airline agreements.
    """
)


# ============================================================================
# DATABASE CONNECTION MANAGEMENT
# ============================================================================

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def generate_booking_reference(airline_code: str) -> str:
    """Generate a unique booking reference"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = random.randint(100, 999)
    return f"{airline_code}-{timestamp}-{random_suffix}"


def calculate_flight_price(
    base_price: float,
    price_multiplier: float,
    is_corporate: bool,
    corporate_discount_percent: float
) -> dict:
    """Calculate final flight price with corporate discount"""
    # Apply dynamic pricing multiplier
    dynamic_price = base_price * price_multiplier
    
    # Apply corporate discount
    discount_amount = 0
    if is_corporate and corporate_discount_percent > 0:
        discount_amount = dynamic_price * (corporate_discount_percent / 100)
    
    final_price = dynamic_price - discount_amount
    
    return {
        "base_price": round(float(base_price), 2),
        "dynamic_price": round(float(dynamic_price), 2),
        "corporate_discount_percent": float(corporate_discount_percent) if is_corporate else 0,
        "corporate_discount_amount": round(float(discount_amount), 2),
        "final_price": round(float(final_price), 2)
    }


# ============================================================================
# TOOL IMPLEMENTATIONS
# ============================================================================

@mcp.tool
def search_flights(
    origin_city: str,
    destination_city: str,
    travel_date: str,
    cabin_class: str = "economy",
    is_corporate: bool = False,
    preferred_airlines_only: bool = False,
    max_price: Optional[float] = None
) -> str:
    """Search for flights between two cities on a specific date.
    
    Args:
        origin_city: Departure city name
        destination_city: Arrival city name
        travel_date: Travel date in YYYY-MM-DD format
        cabin_class: Cabin class - economy, premium_economy, business, or first (default: economy)
        is_corporate: Is this a corporate booking? (default: False)
        preferred_airlines_only: Show only preferred vendor airlines
        max_price: Maximum ticket price
    
    Returns:
        JSON string with available flights and pricing
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Build flight search query
        query = """
            SELECT 
                f.flight_id,
                f.flight_number,
                al.airline_name,
                al.airline_code,
                al.corporate_discount_percent,
                al.is_preferred_vendor,
                al.country as airline_country,
                orig.airport_code as origin_code,
                orig.airport_name as origin_airport,
                orig.city as origin_city,
                orig.country as origin_country,
                dest.airport_code as destination_code,
                dest.airport_name as destination_airport,
                dest.city as destination_city,
                dest.country as destination_country,
                f.departure_time,
                f.arrival_time,
                f.duration_minutes,
                f.aircraft_type,
                fi.base_price,
                fi.price_multiplier,
                fi.available_seats,
                fi.cabin_class
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports orig ON f.origin_airport_id = orig.airport_id
            JOIN airports dest ON f.destination_airport_id = dest.airport_id
            JOIN flight_inventory fi ON f.flight_id = fi.flight_id
            WHERE LOWER(orig.city) = LOWER(%s)
                AND LOWER(dest.city) = LOWER(%s)
                AND fi.flight_date = %s
                AND fi.cabin_class = %s
                AND fi.available_seats > 0
        """
        params = [origin_city, destination_city, travel_date, cabin_class]
        
        if preferred_airlines_only:
            query += " AND al.is_preferred_vendor = TRUE"
        
        query += " ORDER BY fi.base_price * fi.price_multiplier ASC"
        
        cursor.execute(query, params)
        flights = cursor.fetchall()
        
        results = []
        for flight in flights:
            flight_dict = dict(flight)
            
            # Calculate final pricing
            pricing = calculate_flight_price(
                flight["base_price"],
                flight["price_multiplier"],
                is_corporate,
                flight["corporate_discount_percent"]
            )
            
            # Filter by max_price if specified
            if max_price is not None and pricing["final_price"] > max_price:
                continue
            
            flight_dict.update({
                "pricing": pricing,
                "available_seats": flight["available_seats"],
                "duration_hours": round(flight["duration_minutes"] / 60, 1)
            })
            
            # Get baggage allowance
            cursor.execute("""
                SELECT checked_bags, checked_bag_weight_kg, carry_on_bags, carry_on_weight_kg
                FROM baggage_allowance
                WHERE airline_id = (SELECT airline_id FROM flights WHERE flight_id = %s)
                    AND cabin_class = %s
            """, (flight["flight_id"], cabin_class))
            
            baggage = cursor.fetchone()
            if baggage:
                flight_dict["baggage_allowance"] = dict(baggage)
            
            results.append(flight_dict)
    
    return json.dumps({
        "search_criteria": {
            "origin_city": origin_city,
            "destination_city": destination_city,
            "travel_date": travel_date,
            "cabin_class": cabin_class,
            "is_corporate_booking": is_corporate,
            "preferred_only": preferred_airlines_only
        },
        "results_count": len(results),
        "flights": results
    }, indent=2, default=str)


@mcp.tool
def get_flight_details(
    flight_id: int,
    travel_date: str,
    cabin_class: str = "economy",
    is_corporate: bool = False
) -> str:
    """Get detailed information about a specific flight.
    
    Args:
        flight_id: Flight ID
        travel_date: Travel date for pricing and availability
        cabin_class: Cabin class (economy, premium_economy, business, first)
        is_corporate: Is this a corporate booking?
    
    Returns:
        JSON string with complete flight details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get flight details
        cursor.execute("""
            SELECT 
                f.*,
                al.airline_name,
                al.airline_code,
                al.corporate_discount_percent,
                al.is_preferred_vendor,
                al.hub_airport,
                al.country as airline_country,
                orig.airport_code as origin_code,
                orig.airport_name as origin_airport,
                orig.city as origin_city,
                orig.state as origin_state,
                orig.country as origin_country,
                dest.airport_code as destination_code,
                dest.airport_name as destination_airport,
                dest.city as destination_city,
                dest.state as destination_state,
                dest.country as destination_country
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports orig ON f.origin_airport_id = orig.airport_id
            JOIN airports dest ON f.destination_airport_id = dest.airport_id
            WHERE f.flight_id = %s
        """, (flight_id,))
        
        flight = cursor.fetchone()
        if not flight:
            raise ValueError("Flight not found")
        
        flight_dict = dict(flight)
        
        # Get inventory for this flight and date
        cursor.execute("""
            SELECT cabin_class, base_price, price_multiplier, available_seats
            FROM flight_inventory
            WHERE flight_id = %s AND flight_date = %s
            ORDER BY 
                CASE cabin_class
                    WHEN 'economy' THEN 1
                    WHEN 'premium_economy' THEN 2
                    WHEN 'business' THEN 3
                    WHEN 'first' THEN 4
                END
        """, (flight_id, travel_date))
        
        inventory = cursor.fetchall()
        cabin_availability = []
        
        for inv in inventory:
            pricing = calculate_flight_price(
                inv["base_price"],
                inv["price_multiplier"],
                is_corporate,
                flight["corporate_discount_percent"]
            )
            
            # Get baggage for this cabin
            cursor.execute("""
                SELECT * FROM baggage_allowance
                WHERE airline_id = %s AND cabin_class = %s
            """, (flight["airline_id"], inv["cabin_class"]))
            
            baggage = cursor.fetchone()
            
            cabin_availability.append({
                "cabin_class": inv["cabin_class"],
                "available_seats": inv["available_seats"],
                "pricing": pricing,
                "baggage_allowance": dict(baggage) if baggage else None
            })
        
        flight_dict["travel_date"] = travel_date
        flight_dict["cabin_availability"] = cabin_availability
        flight_dict["duration_hours"] = round(flight["duration_minutes"] / 60, 1)
        flight_dict["pricing_is_corporate"] = is_corporate
    
    return json.dumps(flight_dict, indent=2, default=str)


@mcp.tool
def check_seat_availability(
    flight_id: int,
    travel_date: str,
    cabin_class: str = "economy"
) -> str:
    """Check seat availability for a specific flight and cabin class.
    
    Args:
        flight_id: Flight ID
        travel_date: Travel date
        cabin_class: Cabin class (economy, premium_economy, business, first)
    
    Returns:
        JSON string with availability details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get flight and inventory info
        cursor.execute("""
            SELECT 
                f.flight_number,
                al.airline_name,
                al.airline_code,
                orig.city as origin_city,
                dest.city as destination_city,
                fi.available_seats,
                fi.base_price,
                fi.price_multiplier
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports orig ON f.origin_airport_id = orig.airport_id
            JOIN airports dest ON f.destination_airport_id = dest.airport_id
            JOIN flight_inventory fi ON f.flight_id = fi.flight_id
            WHERE f.flight_id = %s
                AND fi.flight_date = %s
                AND fi.cabin_class = %s
        """, (flight_id, travel_date, cabin_class))
        
        result = cursor.fetchone()
        if not result:
            raise ValueError("Flight not found for the specified date and cabin class")
        
        return json.dumps({
            "flight_id": flight_id,
            "airline": result["airline_name"],
            "flight_number": result["flight_number"],
            "route": f"{result['origin_city']} to {result['destination_city']}",
            "travel_date": travel_date,
            "cabin_class": cabin_class,
            "available_seats": result["available_seats"],
            "is_available": result["available_seats"] > 0,
            "current_price": round(float(result["base_price"]) * float(result["price_multiplier"]), 2)
        }, indent=2, default=str)


@mcp.tool
def create_flight_booking(
    flight_id: int,
    travel_date: str,
    passenger_name: str,
    passenger_email: str,
    cabin_class: str = "economy",
    is_corporate: bool = False,
    company_name: Optional[str] = None,
    checked_bags: int = 0,
    purpose_of_travel: Optional[str] = None
) -> str:
    """Create a new flight booking. No pre-registration needed!
    
    Args:
        flight_id: Flight ID
        travel_date: Travel date (YYYY-MM-DD)
        passenger_name: Full name of passenger (first and last name optional)
        passenger_email: Passenger's email address
        cabin_class: Cabin class (economy, premium_economy, business, first)
        is_corporate: Is this a corporate booking? (True/False)
        company_name: Company name (if corporate booking)
        checked_bags: Number of checked bags (default 0)
        purpose_of_travel: Purpose of the trip
    
    Returns:
        JSON string with booking confirmation details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Get or create passenger
            cursor.execute("""
                SELECT passenger_id FROM passengers WHERE LOWER(email) = LOWER(%s)
            """, (passenger_email,))
            
            passenger = cursor.fetchone()
            
            if passenger:
                passenger_id = passenger["passenger_id"]
            else:
                # Create new passenger record
                name_parts = passenger_name.strip().split(' ', 1)
                first_name = name_parts[0]
                last_name = name_parts[1] if len(name_parts) > 1 else ""
                
                # Generate passenger code
                passenger_code = f"{'CORP' if is_corporate else 'INDV'}{random.randint(1000, 9999)}"
                
                cursor.execute("""
                    INSERT INTO passengers (
                        passenger_code, first_name, last_name, email,
                        is_corporate, company_name
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING passenger_id
                """, (passenger_code, first_name, last_name, passenger_email,
                      is_corporate, company_name))
                
                passenger_id = cursor.fetchone()["passenger_id"]
            
            # Get flight and airline info
            cursor.execute("""
                SELECT f.*, al.airline_code, al.corporate_discount_percent
                FROM flights f
                JOIN airlines al ON f.airline_id = al.airline_id
                WHERE f.flight_id = %s
            """, (flight_id,))
            
            flight = cursor.fetchone()
            if not flight:
                raise ValueError("Flight not found")
            
            # Check availability and get pricing
            cursor.execute("""
                SELECT base_price, price_multiplier, available_seats
                FROM flight_inventory
                WHERE flight_id = %s
                    AND flight_date = %s
                    AND cabin_class = %s
            """, (flight_id, travel_date, cabin_class))
            
            inventory = cursor.fetchone()
            if not inventory:
                raise ValueError("No inventory found for this flight and date")
            
            if inventory["available_seats"] < 1:
                raise ValueError("No seats available for this flight")
            
            # Calculate pricing
            pricing = calculate_flight_price(
                inventory["base_price"],
                inventory["price_multiplier"],
                is_corporate,
                flight["corporate_discount_percent"]
            )
            
            # Generate seat number (simplified - random assignment)
            seat_row = random.randint(1, 40)
            seat_letter = random.choice(['A', 'B', 'C', 'D', 'E', 'F'])
            seat_number = f"{seat_row}{seat_letter}"
            
            # Generate booking reference
            booking_ref = generate_booking_reference(flight["airline_code"])
            
            # Create booking
            cursor.execute("""
                INSERT INTO flight_bookings (
                    booking_reference, passenger_id, flight_id, flight_date,
                    cabin_class, seat_number, ticket_price, corporate_discount,
                    checked_bags, booking_status, purpose_of_travel
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                booking_ref, passenger_id, flight_id, travel_date,
                cabin_class, seat_number, pricing["final_price"],
                pricing["corporate_discount_amount"],
                checked_bags, "confirmed", purpose_of_travel or ""
            ))
            
            # Update inventory
            cursor.execute("""
                UPDATE flight_inventory
                SET available_seats = available_seats - 1
                WHERE flight_id = %s
                    AND flight_date = %s
                    AND cabin_class = %s
            """, (flight_id, travel_date, cabin_class))
            
            conn.commit()
            
            # Get route details for response
            cursor.execute("""
                SELECT orig.city as origin, dest.city as destination
                FROM flights f
                JOIN airports orig ON f.origin_airport_id = orig.airport_id
                JOIN airports dest ON f.destination_airport_id = dest.airport_id
                WHERE f.flight_id = %s
            """, (flight_id,))
            route = cursor.fetchone()
            
            return json.dumps({
                "success": True,
                "booking_reference": booking_ref,
                "status": "confirmed",
                "details": {
                    "passenger_name": passenger_name,
                    "passenger_email": passenger_email,
                    "is_corporate": is_corporate,
                    "company": company_name,
                    "flight_number": flight["flight_number"],
                    "route": f"{route['origin']} to {route['destination']}",
                    "travel_date": travel_date,
                    "departure_time": str(flight["departure_time"]),
                    "cabin_class": cabin_class,
                    "seat_number": seat_number,
                    "ticket_price": pricing["final_price"],
                    "corporate_discount": pricing["corporate_discount_amount"],
                    "checked_bags": checked_bags,
                    "purpose": purpose_of_travel or ""
                },
                "message": "Flight booking confirmed successfully"
            }, indent=2, default=str)
        
        except Exception as e:
            conn.rollback()
            raise


@mcp.tool
def get_booking_details(booking_reference: str) -> str:
    """Get complete details of a flight booking by reference number.
    
    Args:
        booking_reference: Booking reference number
    
    Returns:
        JSON string with complete booking details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                fb.*,
                p.first_name || ' ' || p.last_name as passenger_name,
                p.email,
                p.phone,
                p.is_corporate,
                p.company_name,
                al.airline_name,
                al.airline_code,
                f.flight_number,
                f.departure_time,
                f.arrival_time,
                f.duration_minutes,
                f.aircraft_type,
                orig.airport_code as origin_code,
                orig.airport_name as origin_airport,
                orig.city as origin_city,
                dest.airport_code as destination_code,
                dest.airport_name as destination_airport,
                dest.city as destination_city
            FROM flight_bookings fb
            JOIN passengers p ON fb.passenger_id = p.passenger_id
            JOIN flights f ON fb.flight_id = f.flight_id
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports orig ON f.origin_airport_id = orig.airport_id
            JOIN airports dest ON f.destination_airport_id = dest.airport_id
            WHERE fb.booking_reference = %s
        """, (booking_reference,))
        
        booking = cursor.fetchone()
        if not booking:
            raise ValueError("Booking not found")
        
        booking_dict = dict(booking)
        booking_dict["duration_hours"] = round(booking["duration_minutes"] / 60, 1)
        
        # Get baggage allowance for this booking
        cursor.execute("""
            SELECT ba.*
            FROM baggage_allowance ba
            JOIN flights f ON f.airline_id = ba.airline_id
            WHERE f.flight_id = %s AND ba.cabin_class = %s
        """, (booking["flight_id"], booking["cabin_class"]))
        
        baggage = cursor.fetchone()
        if baggage:
            booking_dict["baggage_allowance"] = dict(baggage)
        
        return json.dumps(booking_dict, indent=2, default=str)


@mcp.tool
def list_bookings_by_email(
    passenger_email: str,
    status: Optional[str] = None,
    include_past: bool = False
) -> str:
    """List all flight bookings for a passenger by their email.
    
    Args:
        passenger_email: Passenger's email address
        status: Filter by status (confirmed, cancelled, completed)
        include_past: Include past flights (default false)
    
    Returns:
        JSON string with list of bookings
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get passenger
        cursor.execute("""
            SELECT * FROM passengers WHERE LOWER(email) = LOWER(%s)
        """, (passenger_email,))
        
        passenger = cursor.fetchone()
        if not passenger:
            return json.dumps({
                "message": "No bookings found for this email address",
                "bookings_count": 0,
                "bookings": []
            }, indent=2)
        
        passenger_id = passenger["passenger_id"]
        
        query = """
            SELECT 
                fb.booking_reference,
                fb.booking_status,
                fb.flight_date,
                al.airline_name,
                f.flight_number,
                orig.city as origin_city,
                dest.city as destination_city,
                f.departure_time,
                f.arrival_time,
                fb.cabin_class,
                fb.seat_number,
                fb.ticket_price,
                fb.purpose_of_travel
            FROM flight_bookings fb
            JOIN flights f ON fb.flight_id = f.flight_id
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports orig ON f.origin_airport_id = orig.airport_id
            JOIN airports dest ON f.destination_airport_id = dest.airport_id
            WHERE fb.passenger_id = %s
        """
        params = [passenger_id]
        
        if status:
            query += " AND fb.booking_status = %s"
            params.append(status)
        
        if not include_past:
            query += " AND fb.flight_date >= CURRENT_DATE"
        
        query += " ORDER BY fb.flight_date DESC"
        
        cursor.execute(query, params)
        bookings = [dict(row) for row in cursor.fetchall()]
        
        return json.dumps({
            "passenger_name": f"{passenger['first_name']} {passenger['last_name']}",
            "passenger_email": passenger["email"],
            "is_corporate": bool(passenger["is_corporate"]),
            "company": passenger["company_name"],
            "bookings_count": len(bookings),
            "bookings": bookings
        }, indent=2, default=str)


@mcp.tool
def cancel_flight_booking(
    booking_reference: str,
    passenger_email: str
) -> str:
    """Cancel an existing flight booking and restore seat inventory.
    
    Args:
        booking_reference: Booking reference to cancel
        passenger_email: Passenger's email address (for verification)
    
    Returns:
        JSON string with cancellation confirmation
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Get passenger
            cursor.execute("""
                SELECT passenger_id FROM passengers WHERE LOWER(email) = LOWER(%s)
            """, (passenger_email,))
            
            passenger = cursor.fetchone()
            if not passenger:
                raise ValueError("Passenger not found")
            
            passenger_id = passenger["passenger_id"]
            
            # Get booking
            cursor.execute("""
                SELECT passenger_id, flight_id, flight_date, cabin_class, booking_status
                FROM flight_bookings
                WHERE booking_reference = %s
            """, (booking_reference,))
            
            booking = cursor.fetchone()
            if not booking:
                raise ValueError("Booking not found")
            
            if booking["passenger_id"] != passenger_id:
                raise ValueError("Unauthorized: Booking does not belong to this passenger")
            
            if booking["booking_status"] == "cancelled":
                raise ValueError("Booking is already cancelled")
            
            # Update booking status
            cursor.execute("""
                UPDATE flight_bookings
                SET booking_status = 'cancelled'
                WHERE booking_reference = %s
            """, (booking_reference,))
            
            # Restore seat inventory
            cursor.execute("""
                UPDATE flight_inventory
                SET available_seats = available_seats + 1
                WHERE flight_id = %s
                    AND flight_date = %s
                    AND cabin_class = %s
            """, (booking["flight_id"], booking["flight_date"], booking["cabin_class"]))
            
            conn.commit()
            
            return json.dumps({
                "success": True,
                "booking_reference": booking_reference,
                "status": "cancelled",
                "message": "Flight booking cancelled successfully. Seat inventory restored."
            }, indent=2)
        
        except Exception as e:
            conn.rollback()
            raise


@mcp.tool
def calculate_flight_cost(
    flight_id: int,
    travel_date: str,
    cabin_class: str = "economy",
    is_corporate: bool = False
) -> str:
    """Calculate flight cost with detailed breakdown.
    
    Args:
        flight_id: Flight ID
        travel_date: Travel date
        cabin_class: Cabin class
        is_corporate: Is this a corporate booking?
    
    Returns:
        JSON string with detailed cost breakdown
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get flight and pricing info
        cursor.execute("""
            SELECT 
                al.airline_name,
                al.corporate_discount_percent,
                f.flight_number,
                orig.city as origin_city,
                dest.city as destination_city,
                f.duration_minutes,
                fi.base_price,
                fi.price_multiplier
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports orig ON f.origin_airport_id = orig.airport_id
            JOIN airports dest ON f.destination_airport_id = dest.airport_id
            JOIN flight_inventory fi ON f.flight_id = fi.flight_id
            WHERE f.flight_id = %s
                AND fi.flight_date = %s
                AND fi.cabin_class = %s
        """, (flight_id, travel_date, cabin_class))
        
        info = cursor.fetchone()
        if not info:
            raise ValueError("Flight not found for the specified date and cabin class")
        
        # Calculate pricing
        pricing = calculate_flight_price(
            info["base_price"],
            info["price_multiplier"],
            is_corporate,
            info["corporate_discount_percent"]
        )
        
        return json.dumps({
            "flight": {
                "airline": info["airline_name"],
                "flight_number": info["flight_number"],
                "route": f"{info['origin_city']} to {info['destination_city']}",
                "duration_hours": round(info["duration_minutes"] / 60, 1)
            },
            "travel_details": {
                "travel_date": travel_date,
                "cabin_class": cabin_class
            },
            "cost_breakdown": {
                "base_price": pricing["base_price"],
                "dynamic_price": pricing["dynamic_price"],
                "corporate_discount_percent": pricing["corporate_discount_percent"],
                "corporate_discount_amount": pricing["corporate_discount_amount"],
                "final_price": pricing["final_price"]
            },
            "is_corporate_booking": is_corporate
        }, indent=2, default=str)


@mcp.tool
def get_airlines(country: Optional[str] = None) -> str:
    """Get list of all airlines with their details and corporate discount rates.
    
    Args:
        country: Filter by country (optional)
    
    Returns:
        JSON string with list of airlines
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if country:
            cursor.execute("""
                SELECT 
                    airline_id,
                    airline_code,
                    airline_name,
                    country,
                    corporate_discount_percent,
                    is_preferred_vendor,
                    hub_airport
                FROM airlines
                WHERE LOWER(country) = LOWER(%s)
                ORDER BY airline_name
            """, (country,))
        else:
            cursor.execute("""
                SELECT 
                    airline_id,
                    airline_code,
                    airline_name,
                    country,
                    corporate_discount_percent,
                    is_preferred_vendor,
                    hub_airport
                FROM airlines
                ORDER BY airline_name
            """)
        
        airlines = [dict(row) for row in cursor.fetchall()]
        
        return json.dumps({
            "airlines_count": len(airlines),
            "airlines": airlines
        }, indent=2, default=str)


@mcp.tool
def get_airports(city: Optional[str] = None, country: Optional[str] = None) -> str:
    """Get list of airports, optionally filtered by city or country.
    
    Args:
        city: Filter by city name (optional)
        country: Filter by country (optional)
    
    Returns:
        JSON string with list of airports
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if city:
            cursor.execute("""
                SELECT *
                FROM airports
                WHERE LOWER(city) = LOWER(%s)
                ORDER BY airport_name
            """, (city,))
        elif country:
            cursor.execute("""
                SELECT *
                FROM airports
                WHERE LOWER(country) = LOWER(%s)
                ORDER BY city, airport_name
            """, (country,))
        else:
            cursor.execute("""
                SELECT DISTINCT city, state, country, COUNT(airport_id) as airport_count
                FROM airports
                GROUP BY city, state, country
                ORDER BY city
            """)
        
        airports = [dict(row) for row in cursor.fetchall()]
        
        return json.dumps({
            "airports_count": len(airports),
            "airports": airports
        }, indent=2, default=str)


@mcp.tool
def get_baggage_allowance(airline_code: str, cabin_class: str = "economy") -> str:
    """Get baggage allowance for a specific airline and cabin class.
    
    Args:
        airline_code: Airline IATA code (e.g., AA, DL, UA)
        cabin_class: Cabin class (economy, premium_economy, business, first)
    
    Returns:
        JSON string with baggage allowance details
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                al.airline_name,
                al.airline_code,
                ba.cabin_class,
                ba.checked_bags,
                ba.checked_bag_weight_kg,
                ba.carry_on_bags,
                ba.carry_on_weight_kg
            FROM baggage_allowance ba
            JOIN airlines al ON ba.airline_id = al.airline_id
            WHERE al.airline_code = %s AND ba.cabin_class = %s
        """, (airline_code, cabin_class))
        
        result = cursor.fetchone()
        if not result:
            raise ValueError("Baggage allowance not found for this airline and cabin class")
        
        return json.dumps(dict(result), indent=2, default=str)


@mcp.tool
def get_route_options(origin_city: str, destination_city: str) -> str:
    """Get all available route options between two cities with airline information.
    
    Args:
        origin_city: Departure city
        destination_city: Arrival city
    
    Returns:
        JSON string with available routes and airlines
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT
                al.airline_name,
                al.airline_code,
                al.is_preferred_vendor,
                al.corporate_discount_percent,
                al.country as airline_country,
                f.flight_number,
                f.flight_id,
                orig.airport_code as origin_code,
                orig.airport_name as origin_airport,
                dest.airport_code as destination_code,
                dest.airport_name as destination_airport,
                f.departure_time,
                f.arrival_time,
                f.duration_minutes,
                f.aircraft_type
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports orig ON f.origin_airport_id = orig.airport_id
            JOIN airports dest ON f.destination_airport_id = dest.airport_id
            WHERE LOWER(orig.city) = LOWER(%s)
                AND LOWER(dest.city) = LOWER(%s)
            ORDER BY al.airline_name, f.departure_time
        """, (origin_city, destination_city))
        
        routes = [dict(row) for row in cursor.fetchall()]
        
        if not routes:
            return json.dumps({
                "message": f"No direct flights found between {origin_city} and {destination_city}",
                "routes_count": 0,
                "routes": []
            }, indent=2)
        
        for route in routes:
            route["duration_hours"] = round(route["duration_minutes"] / 60, 1)
        
        return json.dumps({
            "route": f"{origin_city} to {destination_city}",
            "routes_count": len(routes),
            "routes": routes
        }, indent=2, default=str)


# ============================================================================
# MAIN SERVER ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    logger.info(f"Starting Airline Booking FastMCP Server (PostgreSQL)")
    # logger.info(f"Database: {DB_URL.split('@')[1].split('/')[0]}")  # Show host only
    
    mcp.run(transport="http")
