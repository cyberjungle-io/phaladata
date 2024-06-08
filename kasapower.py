import kasa
import asyncio

# Create a SmartDevice object for the Kasa smart strip.
strip = kasa.SmartStrip("10.2.1.81")
#asyncio.run(strip.update())
print(strip.alias)
print(asyncio.run(strip.children[5].current_consumption()))




