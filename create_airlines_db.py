import psycopg2
import psycopg2.extras
import datetime
import random
import os

def create_airline_booking_db_postgres(db_url):
    """
    Creates and populates a PostgreSQL database for an airline booking system.
    This function sets up the initial schema and a small sample of US-based data.
    """
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        print("Successfully connected to PostgreSQL to create initial schema.")

        sql_script = """
        DROP VIEW IF EXISTS flight_booking_details_view;
        DROP TABLE IF EXISTS flight_bookings; DROP TABLE IF EXISTS flight_inventory; DROP TABLE IF EXISTS baggage_allowance;
        DROP TABLE IF EXISTS flights; DROP TABLE IF EXISTS airports; DROP TABLE IF EXISTS airlines; DROP TABLE IF EXISTS passengers;

        CREATE TABLE passengers (
            passenger_id SERIAL PRIMARY KEY, passenger_code TEXT UNIQUE NOT NULL, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL, phone TEXT, date_of_birth DATE, passport_number TEXT, is_corporate BOOLEAN DEFAULT FALSE,
            company_name TEXT, frequent_flyer_number TEXT
        );
        CREATE TABLE airlines (
            airline_id SERIAL PRIMARY KEY, airline_code TEXT UNIQUE NOT NULL, airline_name TEXT NOT NULL, country TEXT NOT NULL,
            corporate_discount_percent NUMERIC(5, 2) DEFAULT 0, is_preferred_vendor BOOLEAN DEFAULT FALSE, hub_airport TEXT
        );
        CREATE TABLE airports (
            airport_id SERIAL PRIMARY KEY, airport_code TEXT UNIQUE NOT NULL, airport_name TEXT NOT NULL, city TEXT NOT NULL,
            state TEXT, country TEXT NOT NULL, timezone TEXT, latitude NUMERIC(9, 6), longitude NUMERIC(9, 6)
        );
        CREATE TABLE flights (
            flight_id SERIAL PRIMARY KEY, airline_id INTEGER REFERENCES airlines(airline_id) ON DELETE CASCADE, flight_number TEXT NOT NULL,
            origin_airport_id INTEGER REFERENCES airports(airport_id), destination_airport_id INTEGER REFERENCES airports(airport_id),
            departure_time TIME NOT NULL, arrival_time TIME NOT NULL, duration_minutes INTEGER NOT NULL,
            aircraft_type TEXT, total_seats INTEGER DEFAULT 180, UNIQUE(airline_id, flight_number)
        );
        CREATE TABLE baggage_allowance (
            baggage_id SERIAL PRIMARY KEY, airline_id INTEGER REFERENCES airlines(airline_id) ON DELETE CASCADE, cabin_class TEXT NOT NULL,
            checked_bags INTEGER DEFAULT 0, checked_bag_weight_kg INTEGER DEFAULT 23, carry_on_bags INTEGER DEFAULT 1, carry_on_weight_kg INTEGER DEFAULT 7
        );
        CREATE TABLE flight_inventory (
            inventory_id SERIAL PRIMARY KEY, flight_id INTEGER REFERENCES flights(flight_id) ON DELETE CASCADE, flight_date DATE NOT NULL,
            cabin_class TEXT NOT NULL, base_price NUMERIC(10, 2) NOT NULL, available_seats INTEGER DEFAULT 0,
            price_multiplier NUMERIC(4, 2) DEFAULT 1.0, UNIQUE(flight_id, flight_date, cabin_class)
        );
        CREATE TABLE flight_bookings (
            booking_id SERIAL PRIMARY KEY, booking_reference TEXT UNIQUE NOT NULL, passenger_id INTEGER REFERENCES passengers(passenger_id),
            flight_id INTEGER REFERENCES flights(flight_id), flight_date DATE NOT NULL, cabin_class TEXT NOT NULL, seat_number TEXT,
            ticket_price NUMERIC(10, 2) NOT NULL, corporate_discount NUMERIC(10, 2) DEFAULT 0, checked_bags INTEGER DEFAULT 0,
            booking_status TEXT DEFAULT 'confirmed', purpose_of_travel TEXT, booked_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX idx_passengers_email ON passengers(email); CREATE INDEX idx_flights_route ON flights(origin_airport_id, destination_airport_id);
        CREATE INDEX idx_inventory_date ON flight_inventory(flight_date); CREATE INDEX idx_bookings_passenger ON flight_bookings(passenger_id);

        INSERT INTO passengers (passenger_code, first_name, last_name, email, is_corporate, company_name, frequent_flyer_number) VALUES
        ('CORP001', 'John', 'Smith', 'john.smith@techcorp.com', TRUE, 'TechCorp Inc.', 'TC123456'),
        ('CORP002', 'Jane', 'Doe', 'jane.doe@innovate.io', TRUE, 'Innovate Solutions', 'IS789012'),
        ('INDV001', 'Mary', 'Williams', 'mary.w@email.com', FALSE, NULL, NULL);

        INSERT INTO airlines (airline_code, airline_name, country, corporate_discount_percent, is_preferred_vendor, hub_airport) VALUES
        ('AA', 'American Airlines', 'USA', 15.00, TRUE, 'DFW'), ('DL', 'Delta Air Lines', 'USA', 18.00, TRUE, 'ATL'),
        ('UA', 'United Airlines', 'USA', 20.00, TRUE, 'ORD'), ('WN', 'Southwest Airlines', 'USA', 10.00, FALSE, 'DAL');

        INSERT INTO airports (airport_code, airport_name, city, state, country, timezone, latitude, longitude) VALUES
        ('JFK', 'John F. Kennedy Intl', 'New York', 'NY', 'USA', 'America/New_York', 40.6413, -73.7781),
        ('LAX', 'Los Angeles Intl', 'Los Angeles', 'CA', 'USA', 'America/Los_Angeles', 33.9416, -118.4085),
        ('ORD', 'O''Hare Intl', 'Chicago', 'IL', 'USA', 'America/Chicago', 41.9742, -87.9073),
        ('DFW', 'Dallas/Fort Worth Intl', 'Dallas', 'TX', 'USA', 'America/Chicago', 32.8998, -97.0403),
        ('DEN', 'Denver Intl', 'Denver', 'CO', 'USA', 'America/Denver', 39.8561, -104.6737),
        ('SFO', 'San Francisco Intl', 'San Francisco', 'CA', 'USA', 'America/Los_Angeles', 37.6213, -122.3790),
        ('ATL', 'Hartsfield-Jackson Atlanta Intl', 'Atlanta', 'GA', 'USA', 'America/New_York', 33.6407, -84.4277);

        INSERT INTO flights (airline_id, flight_number, origin_airport_id, destination_airport_id, departure_time, arrival_time, duration_minutes, aircraft_type, total_seats) VALUES
        (1, 'AA100', 1, 2, '08:00', '11:30', 360, 'Boeing 777', 310), (2, 'DL500', 7, 2, '07:30', '11:00', 330, 'Airbus A330', 290),
        (3, 'UA400', 3, 6, '09:00', '11:30', 270, 'Boeing 787', 250);

        CREATE VIEW flight_booking_details_view AS
        SELECT fb.booking_id, fb.booking_reference, p.first_name || ' ' || p.last_name as passenger_name, p.is_corporate, p.company_name,
            al.airline_name, f.flight_number, orig.airport_code as origin_code, dest.airport_code as destination_code,
            fb.flight_date, f.departure_time, fb.ticket_price, fb.booking_status
        FROM flight_bookings fb JOIN passengers p ON fb.passenger_id = p.passenger_id JOIN flights f ON fb.flight_id = f.flight_id
        JOIN airlines al ON f.airline_id = al.airline_id JOIN airports orig ON f.origin_airport_id = orig.airport_id
        JOIN airports dest ON f.destination_airport_id = dest.airport_id;
        """
        for statement in sql_script.split(';'):
            if statement.strip():
                cursor.execute(statement)
        print("Initial schema and sample US data created successfully.")
        conn.commit()
    except psycopg2.Error as e:
        print(f"Database error during initial setup: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def populate_flight_inventory(cursor, days=90):
    """Populates the flight_inventory for all existing flights for a number of days."""
    print(f"\nPopulating flight inventory for the next {days} days...")
    cursor.execute("SELECT flight_id, total_seats FROM flights")
    all_flights = cursor.fetchall()

    cabin_class_pricing = {'economy': 1.0, 'premium_economy': 1.8, 'business': 3.5, 'first': 5.0}
    seat_distribution = {'economy': 0.70, 'premium_economy': 0.15, 'business': 0.10, 'first': 0.05}

    inventory_records = []
    start_date = datetime.date.today()
    for flight_id, total_seats in all_flights:
        economy_base_price = random.uniform(150, 700) # Increased range for international
        for i in range(days):
            flight_date = start_date + datetime.timedelta(days=i)
            price_multiplier = 1.0 + (random.choice([-1, 1]) * (i / 180.0)) # Fluctuate price over time
            if flight_date.weekday() in [4, 5, 6]: price_multiplier *= 1.2

            for cabin_class, class_multiplier in cabin_class_pricing.items():
                if total_seats < 200 and cabin_class in ['premium_economy', 'first']: continue

                available_seats = int(total_seats * seat_distribution[cabin_class] * random.uniform(0.3, 1.0))
                inventory_records.append((flight_id, flight_date, cabin_class, round(economy_base_price * class_multiplier, 2), available_seats, round(price_multiplier, 2)))

    psycopg2.extras.execute_batch(cursor, "INSERT INTO flight_inventory (flight_id, flight_date, cabin_class, base_price, available_seats, price_multiplier) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (flight_id, flight_date, cabin_class) DO NOTHING", inventory_records)
    # Corrected logging to show the number of records attempted, not cursor.rowcount for batch
    print(f"Attempted to insert {len(inventory_records)} inventory records.")

def add_international_data(db_url):
    """Adds a set of international airlines, airports, and flights to the database."""
    print("\nAdding international airlines and airports...")
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        airlines_data = [
            ('BA', 'British Airways', 'United Kingdom', 15.0, True, 'LHR'), ('LH', 'Lufthansa', 'Germany', 16.0, True, 'FRA'),
            ('AF', 'Air France', 'France', 14.0, True, 'CDG'), ('EK', 'Emirates', 'UAE', 12.0, True, 'DXB'),
            ('QF', 'Qantas', 'Australia', 10.0, False, 'SYD'), ('SQ', 'Singapore Airlines', 'Singapore', 18.0, True, 'SIN'),
            ('CX', 'Cathay Pacific', 'Hong Kong', 13.0, False, 'HKG'), ('KE', 'Korean Air', 'South Korea', 11.0, False, 'ICN'),
            ('JL', 'Japan Airlines', 'Japan', 14.0, True, 'HND'), ('AC', 'Air Canada', 'Canada', 12.0, True, 'YYZ'),
            ('KL', 'KLM Royal Dutch Airlines', 'Netherlands', 13.0, True, 'AMS'), ('QR', 'Qatar Airways', 'Qatar', 15.0, True, 'DOH'),
            ('TK', 'Turkish Airlines', 'Turkey', 10.0, False, 'IST'), ('IB', 'Iberia', 'Spain', 9.0, False, 'MAD'),
            ('AI', 'Air India', 'India', 8.0, False, 'DEL'), ('ET', 'Ethiopian Airlines', 'Ethiopia', 7.0, False, 'ADD'),
            ('SA', 'South African Airways', 'South Africa', 6.0, False, 'JNB'), ('LA', 'LATAM Airlines', 'Chile', 9.0, False, 'SCL'),
            ('NZ', 'Air New Zealand', 'New Zealand', 10.0, False, 'AKL'), ('EY', 'Etihad Airways', 'UAE', 12.0, True, 'AUH')
        ]

        airports_data = [
            ('LHR', 'Heathrow Airport', 'London', None, 'United Kingdom', 'Europe/London', 51.4700, -0.4543),
            ('CDG', 'Charles de Gaulle Airport', 'Paris', None, 'France', 'Europe/Paris', 49.0097, 2.5479),
            ('FRA', 'Frankfurt Airport', 'Frankfurt', None, 'Germany', 'Europe/Berlin', 50.0379, 8.5622),
            ('AMS', 'Amsterdam Airport Schiphol', 'Amsterdam', None, 'Netherlands', 'Europe/Amsterdam', 52.3105, 4.7683),
            ('DXB', 'Dubai International Airport', 'Dubai', None, 'UAE', 'Asia/Dubai', 25.2532, 55.3657),
            ('HND', 'Haneda Airport', 'Tokyo', None, 'Japan', 'Asia/Tokyo', 35.5494, 139.7798),
            ('HKG', 'Hong Kong International Airport', 'Hong Kong', None, 'China', 'Asia/Hong_Kong', 22.3080, 113.9185),
            ('SIN', 'Singapore Changi Airport', 'Singapore', None, 'Singapore', 'Asia/Singapore', 1.3644, 103.9915),
            ('SYD', 'Sydney Kingsford Smith Airport', 'Sydney', 'NSW', 'Australia', 'Australia/Sydney', -33.9399, 151.1753),
            ('YYZ', 'Toronto Pearson International Airport', 'Toronto', 'ON', 'Canada', 'America/Toronto', 43.6777, -79.6248),
            ('ICN', 'Incheon International Airport', 'Seoul', None, 'South Korea', 'Asia/Seoul', 37.4602, 126.4407),
            ('DOH', 'Hamad International Airport', 'Doha', None, 'Qatar', 'Asia/Qatar', 25.2731, 51.6081),
            ('IST', 'Istanbul Airport', 'Istanbul', None, 'Turkey', 'Europe/Istanbul', 41.2753, 28.7519),
            ('DEL', 'Indira Gandhi International Airport', 'Delhi', None, 'India', 'Asia/Kolkata', 28.5562, 77.1000),
            ('JNB', 'O. R. Tambo International Airport', 'Johannesburg', None, 'South Africa', 'Africa/Johannesburg', -26.1392, 28.2460),
            ('SCL', 'Arturo Merino Benítez Airport', 'Santiago', None, 'Chile', 'America/Santiago', -33.3930, -70.7858),
            ('GRU', 'São Paulo/Guarulhos Airport', 'São Paulo', 'SP', 'Brazil', 'America/Sao_Paulo', -23.4356, -46.4731),
            ('AKL', 'Auckland Airport', 'Auckland', None, 'New Zealand', 'Pacific/Auckland', -37.0082, 174.7917),
            ('NRT', 'Narita International Airport', 'Tokyo', None, 'Japan', 'Asia/Tokyo', 35.7719, 140.3928),
            ('BCN', 'Barcelona–El Prat Airport', 'Barcelona', None, 'Spain', 'Europe/Madrid', 41.2974, 2.0833)
        ]

        psycopg2.extras.execute_batch(cursor, "INSERT INTO airlines (airline_code, airline_name, country, corporate_discount_percent, is_preferred_vendor, hub_airport) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (airline_code) DO NOTHING", airlines_data)
        psycopg2.extras.execute_batch(cursor, "INSERT INTO airports (airport_code, airport_name, city, state, country, timezone, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (airport_code) DO NOTHING", airports_data)
        print(f"Processed {len(airlines_data)} airlines and {len(airports_data)} airports.")
        conn.commit()

        print("\nAdding new international flight routes...")
        cursor.execute("SELECT airline_id FROM airlines WHERE country != 'USA'")
        international_airlines = [r[0] for r in cursor.fetchall()]
        cursor.execute("SELECT airport_id FROM airports")
        all_airports = [r[0] for r in cursor.fetchall()]

        new_flights = []
        for i in range(100): # Add 100 new routes
            origin, dest = random.sample(all_airports, 2)
            airline_id = random.choice(international_airlines)
            new_flights.append((
                airline_id, f'INT{1000+i}', origin, dest, f"{random.randint(0,23):02d}:{random.randint(0,59):02d}", f"{random.randint(0,23):02d}:{random.randint(0,59):02d}",
                random.randint(300, 900), random.choice(['Boeing 787', 'Airbus A350', 'Boeing 777']), random.randint(250, 400)
            ))
        psycopg2.extras.execute_batch(cursor, "INSERT INTO flights (airline_id, flight_number, origin_airport_id, destination_airport_id, departure_time, arrival_time, duration_minutes, aircraft_type, total_seats) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (airline_id, flight_number) DO NOTHING", new_flights)
        print(f"Attempted to insert {len(new_flights)} new international flights.")
        conn.commit()

        populate_flight_inventory(cursor)
        conn.commit()

    except psycopg2.Error as e:
        print(f"Database error while adding international data: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def add_more_passengers(db_url, num_passengers=100):
    """Adds a specified number of randomly generated passengers to the database."""
    print(f"\nAdding {num_passengers} new passengers...")
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        first_names = ['Robert', 'Jennifer', 'James', 'Patricia', 'Linda', 'William', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Karen', 'Charles', 'Nancy']
        last_names = ['Johnson', 'Garcia', 'Martinez', 'Rodriguez', 'Lee', 'Walker', 'Hall', 'Allen', 'Young', 'Hernandez', 'King', 'Wright', 'Lopez', 'Hill']
        companies = ['Quantum Leap', 'Stellar Solutions', 'Apex Innovations', 'Nexus Corp', 'Visionary Inc.', 'Pioneer Group']

        cursor.execute("SELECT count(*) FROM passengers")
        pax_count = cursor.fetchone()[0]

        new_pax = []
        for i in range(num_passengers):
            first, last = random.choice(first_names), random.choice(last_names)
            email = f"{first.lower()}.{last.lower()}{pax_count+i}@example.com"
            is_corp = random.choice([True, False])
            company = random.choice(companies) if is_corp else None
            new_pax.append((f"PAX{pax_count+i:04d}", first, last, email, is_corp, company))

        psycopg2.extras.execute_batch(cursor, "INSERT INTO passengers (passenger_code, first_name, last_name, email, is_corporate, company_name) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING", new_pax)
        print(f"Attempted to insert {len(new_pax)} new passengers.")
        conn.commit()
    except psycopg2.Error as e:
        print(f"Database error while adding passengers: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def add_more_bookings(db_url, num_bookings=200):
    """Adds a specified number of randomly generated bookings using an efficient query strategy."""
    print(f"\nAdding {num_bookings} new bookings...")
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Fetch data into memory. These tables are small enough.
        cursor.execute("SELECT p.* FROM passengers p")
        passengers = cursor.fetchall()
        cursor.execute("""
            SELECT f.flight_id, a.airline_id, a.corporate_discount_percent
            FROM flights f
            JOIN airlines a ON f.airline_id = a.airline_id
        """)
        flights = cursor.fetchall()

        cursor.execute("SELECT MAX(booking_id) from flight_bookings")
        booking_counter = (cursor.fetchone()[0] or 0) + 1

        new_bookings = []
        cabin_classes = ['economy', 'premium_economy', 'business', 'first']

        # Loop to create bookings one by one
        while len(new_bookings) < num_bookings:
            passenger = random.choice(passengers)
            flight = random.choice(flights)
            flight_date = datetime.date.today() + datetime.timedelta(days=random.randint(1, 89))
            cabin_class = random.choice(cabin_classes)

            # Targeted query for an available seat
            cursor.execute("""
                SELECT base_price, price_multiplier FROM flight_inventory
                WHERE flight_id = %s AND flight_date = %s AND cabin_class = %s AND available_seats > 0
            """, (flight['flight_id'], flight_date, cabin_class))

            seat_info = cursor.fetchone()

            if seat_info:
                ticket_price = seat_info['base_price'] * seat_info['price_multiplier']
                discount = 0
                if passenger['is_corporate'] and flight['corporate_discount_percent'] > 0:
                    discount = ticket_price * (flight['corporate_discount_percent'] / 100)

                new_bookings.append((
                    f"GEN-{booking_counter + len(new_bookings):06d}", passenger['passenger_id'], flight['flight_id'], flight_date, cabin_class,
                    f"{random.randint(1,40)}{random.choice('ABCDEF')}", round(ticket_price - discount, 2), round(discount, 2),
                    random.choice(['confirmed', 'completed', 'cancelled']), random.choice(['Business', 'Leisure', 'Conference'])
                ))

        if new_bookings:
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO flight_bookings (booking_reference, passenger_id, flight_id, flight_date, cabin_class, seat_number,
                ticket_price, corporate_discount, booking_status, purpose_of_travel)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", new_bookings)
            print(f"Successfully inserted {len(new_bookings)} new bookings.")
            conn.commit()
        else:
            print("Could not generate any new bookings. Check inventory.")

    except psycopg2.Error as e:
        print(f"Database error while adding bookings: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


if __name__ == '__main__':
    NEON_DB_URL = ''

    # 1. Reset and create the base schema with minimal data
    create_airline_booking_db_postgres(NEON_DB_URL)

    # 2. Add international airlines, airports, and flight routes
    add_international_data(NEON_DB_URL)

    # 3. Add more passengers to the system
    add_more_passengers(NEON_DB_URL, num_passengers=100)

    # 4. Add 200 new bookings using the efficient method
    add_more_bookings(NEON_DB_URL, num_bookings=200)

    print("\nAirline database population complete.")