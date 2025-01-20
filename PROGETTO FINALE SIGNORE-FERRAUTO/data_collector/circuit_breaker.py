import time
import threading


class CircuitBreakerOpenException(Exception):
    def __init__(self, message="Circuito aperto. Chiamate a funzione negate."):
        
        super().__init__(message)


class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30, expected_exception=Exception):
        """
        Initializes a new instance of the CircuitBreaker class.

        Parameters:
        - failure_threshold (int): Number of consecutive failures allowed before opening the circuit.
        - recovery_timeout (int): Time in seconds to wait before attempting to reset the circuit.
        - expected_exception (Exception): The exception type that triggers a failure count increment.
        """
        self.failure_threshold = failure_threshold          # Threshold for failures to open the circuit
        self.recovery_timeout = recovery_timeout            # Timeout before attempting to reset the circuit
        self.expected_exception = expected_exception        # Exception type to monitor
        self.failure_count = 0                              # Counter for consecutive failures
        self.last_failure_time = None                       # Timestamp of the last failure
        self.state = 'CLOSED'                               # Initial state of the circuit
        self.lock = threading.Lock()                        # Lock to ensure thread-safe operations
# ---------------------------------------------------------------------------------

    def call(self, func, *args, **kwargs):
        """
        Esegue la funzione con in aggiunta il meccanismo del circuit breaker..

        Parametri:

        - func (callable): La funzione da eseguire.
        *args: Lista di argomenti di lunghezza variabile per la funzione.
        **kwargs: Argomenti opzionali passati come parole chiave alla funzione.
        
        Valori restituiti:

        - Il risultato dell'esecuzione della funzione, se ha successo.
        
        Eccezioni sollevate:

        - CircuitBreakerOpenException: Se il circuito è aperto e le chiamate non sono permesse.
        - Exception: Rilancia eventuali eccezioni generate dalla funzione.
        """
        with self.lock:  # Ensure thread-safe access
            if self._is_circuit_open():
                raise CircuitBreakerOpenException()
            
        # Esecuzione di func fuori dal lock per evitare ritardi eccessivi.
        try:
            result = func(*args, **kwargs)
        except self.expected_exception as e:
            self._handle_failure()
            raise e  # Re-raise the exception to the caller
        else:
            self._handle_success()
            return result

    def _is_circuit_open(self):
        """
        Controlla lo stato del circuito e determina se è aperto.

        Valore restituito:
        - bool: True se il circuito è aperto e quindi devia le chiamate a func, False altrimenti.
        """
        if self.state == 'OPEN':
            if (time.time() - self.last_failure_time) > self.recovery_timeout:
                # Transition to HALF_OPEN state after recovery timeout
                self.state = 'HALF_OPEN'
                return False
            return True  # Circuit is still open
        return False

    def _handle_failure(self):
        """
        Gestisce un fallimento della chiamata func.
        """
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'  # Open the circuit

    def _handle_success(self):
        """
        Gestisce un successo della chiamata func.
        """
        with self.lock:
            if self.state == 'HALF_OPEN':
                # Reset circuit to CLOSED on success in HALF_OPEN state
                self.state = 'CLOSED'
                self.failure_count = 0


