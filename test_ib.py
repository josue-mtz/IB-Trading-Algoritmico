from ib_insync import IB

ib = IB()
try:
    # Conéctate a IB Gateway (puerto 4002 para Paper) o TWS (7497 para Paper)
    ib.connect('127.0.0.1', 4002, clientId=1)
    print("✅ ¡Conexión exitosa a Interactive Brokers!")
    # Muestra las cuentas disponibles para confirmar que es la demo
    print(f"Cuentas disponibles: {ib.managedAccounts()}")
except Exception as e:
    print(f"❌ Error de conexión: {e}")
    print("Verifica que IB Gateway/TWS esté abierto, la sesión Paper activa y el puerto correcto.")
finally:
    ib.disconnect()