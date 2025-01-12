import grpc
import financial_service_pb2 as pb2
import financial_service_pb2_grpc as pb2_grpc
import uuid
from google.protobuf.wrappers_pb2 import FloatValue

def mostra_menu(client_id):
    menu = [
        "1) Registra utente",
        "2) Aggiorna utente",
        "3) Cancella utente",
        "4) Leggi ultimo valore",
        "5) Calcola valore medio",
        "0) Termina"
    ]
    print(f"\nClient ID: {client_id}")
    print("=" * 25)
    print("           MENU")
    print("=" * 25)
    for voce in menu:
        print(f"{voce.center(25)}")
    print("=" * 25)




def run():
    # Connessione al server gRPC
    target = 'localhost:50051'

    # Generazione casuale id utente e inizializzazione request_id = 0
    client_id = str(uuid.uuid4())
    request_id = 0 

    with grpc.insecure_channel(target) as channel:    
        stub = pb2_grpc.FinancialServiceStub(channel)

        
        # Definisco il dizionario di azioni
        azioni = {
            1: ("Registra Utente", lambda: pb2.UserRequest(
                email=input("\nInserisci email: "),
                ticker=input("\nInserisci ticker: "),
                low_value = FloatValue(value = float(lv)) if (lv := input("\nInserisci low value (opzionale): ")) else None,
                high_value = FloatValue(value = float(hv)) if (hv := input("\nInserisci high value (opzionale): ")) else None
            ), stub.RegisterUser),

            
            2: ("Aggiorna Utente", lambda: pb2.UserRequest(
                email=input("\nInserisci email: "),
                ticker=input("\nInserisci nuovo ticker: "),
                low_value = FloatValue(value = float(lv)) if (lv := input("\nInserisci nuovo low value (lascia vuoto per toglierlo): ")) else None,
                high_value = FloatValue(value = float(hv)) if (hv := input("\nInserisci nuovo high value (lascia vuoto per toglierlo): ")) else None,
            ), stub.UpdateUser),
            
            3: ("Cancella Utente", lambda: pb2.UserRequest(
                email=input("\nInserisci email: ")
            ), stub.DeleteUser),
            
            4: ("Leggi ultimo valore", lambda: pb2.UserRequest(
                email=input("\nInserisci email: ")
            ), stub.GetLatestValue),
            
            5: ("Calcola valore medio", lambda: pb2.StockHistoryRequest(
                email=input("\nInserisci email: "),
                count=int(input("\nInserisci il numero di valori: "))
            ), stub.GetAverageValue)
        }


        
        while True:
          
            mostra_menu(client_id)
            scelta = int(input("Inserisci il numero dell'opzione (0-5): "))
            my_metadata = [('client_id', client_id), ('request_id', str(request_id))]

            if scelta == 0:
                print("\nClient terminato.")
                break


            if scelta in azioni:
                titolo, richiesta_funzione, funzione_rpc = azioni[scelta]
                print(f"\n=== {titolo} ===")
                try:
                    richiesta = richiesta_funzione()
                    risposta = funzione_rpc(richiesta, metadata=my_metadata)

                    if not risposta.success:
                        print(f"Errore: {risposta.message} (Esito: {risposta.success})")
                    elif scelta == 4:
                        print(f"ULITMO RECORD\n{'='*40}")
                        print(f"Ticker: {risposta.data.ticker}")
                        print(f"Data: {risposta.data.date}")
                        print(f"\n{'='*40}")
                        print(f"{'Dato':<12} {'Valore':<10}")
                        print(f"{'Open':<12}: {risposta.data.open:,.2f}")
                        print(f"{'High':<12}: {risposta.data.high:,.2f}")
                        print(f"{'Low':<12}: {risposta.data.low:,.2f}")
                        print(f"{'Close':<12}: {risposta.data.close:,.2f}")
                        print(f"{'Volume':<12}: {risposta.data.volume:,}")
                        print(f"{'Dividends':<12}: {risposta.data.dividends:,.2f}")
                        print(f"{'Splits':<12}: {risposta.data.splits:,.2f}")
                        print(f"\n{'='*40}")
                        print(f"Esito: {risposta.success}")
                        print(f"{'='*40}")

                    elif scelta == 5:
                        print(f"VALORI MEDI CALCOLATI SUGLI ULTIMI {richiesta.count} RECORD\n{'='*40}")
                        print(f"{'Dato':<12} {'Valore':<10}")
                        print(f"{'Open':<12}: {risposta.data.open:,.2f}")
                        print(f"{'High':<12}: {risposta.data.high:,.2f}")
                        print(f"{'Low':<12}: {risposta.data.low:,.2f}")
                        print(f"{'Close':<12}: {risposta.data.close:,.2f}")
                        print(f"{'Volume':<12}: {risposta.data.volume:,}")
                        print(f"{'Dividends':<12}: {risposta.data.dividends:,.2f}")
                        print(f"{'Splits':<12}: {risposta.data.splits:,.2f}")
                        print(f"\n{'='*40}")
                        print(f"Esito: {risposta.success}")
                        print(f"{'='*40}")
                            
                    else:
                        print(f"Messaggio di risposta: {risposta.message} (Esito: {risposta.success})")
                except (ValueError, TypeError) as e:
                    print(f"Errore:", str(e))
                request_id += 1
            else:
                print("\nOpzione non valida! Inserisci un numero tra 0 e 5.")




if __name__ == "__main__":
    run()


