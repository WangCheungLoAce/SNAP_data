# Custom retry logic with exponential backoff
def custom_retry(func):
    max_attempts = 2
    wait_min = 4
    wait_max = 10

    def wrapper(*args, **kwargs):
        attempts = 0
        while attempts < max_attempts:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"Error: {e}. Retrying...")
                time.sleep(wait_min + (wait_max - wait_min) * attempts / max_attempts)
                attempts += 1
        print(f"Maximum retry attempts ({max_attempts}) reached. Returning None.")
        return None, None

    return wrapper

# Function to get latitude and longitude from address with custom retry logic
@custom_retry
def get_lat_long(address):
    geolocator = Nominatim(user_agent="my_geocoder")
    location = geolocator.geocode(address, timeout=10)  # Increase timeout to 10 seconds
    if location:
        return location.latitude, location.longitude
    else:
        return None, None

# Function to update latitude and longitude if either is 0
def update_lat_long(row):
    if row['Latitude'] == 0 or row['Longitude'] == 0:
        # Construct the address from address components
        address = (
            f"{row['Street Number']} {row['Street Name']}, "
            f"{row['City']}, {row['State']} {row['Zip Code']}"
        )
        try:
            # Get new latitude and longitude with custom retry logic
            new_lat, new_long = get_lat_long(address)
            if new_lat is not None and new_long is not None:
                # Update the latitude and longitude values
                row['Latitude'] = new_lat
                row['Longitude'] = new_long
        except Exception as e:
            print(f"Error geocoding address: {address} - {e}")
    return row

# Prompt user for confirmation
proceed = input("Do you want to update latitude and longitude where necessary? (yes/no): ").lower()

if proceed == "yes":
    # Apply the update_lat_long function to the DataFrame with tqdm progress bar
    tqdm.pandas()
    raw_df = raw_df.progress_apply(update_lat_long, axis=1)

    print("Latitude and Longitude values updated where necessary.")
else:
    print("No updates performed.")