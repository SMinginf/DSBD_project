-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Creato il: Nov 23, 2024 alle 12:08
-- Versione del server: 10.4.28-MariaDB
-- Versione PHP: 8.2.4

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `dsbd_hw1`
--

-- --------------------------------------------------------

--
-- Struttura della tabella `dati`
--

CREATE TABLE `dati` (
  `id` int(11) NOT NULL,
  `ticker` varchar(45) NOT NULL,
  `date` datetime NOT NULL,
  `open` double NOT NULL,
  `high` double NOT NULL,
  `low` double NOT NULL,
  `close` double NOT NULL,
  `volume` int(11) NOT NULL,
  `dividends` decimal(3,2) NOT NULL,
  `splits` decimal(2,1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dump dei dati per la tabella `dati`
--

INSERT INTO `dati` (`id`, `ticker`, `date`, `open`, `high`, `low`, `close`, `volume`, `dividends`, `splits`) VALUES
(20, 'MSFT', '2024-11-21 00:00:00', 419.5, 419.7799987792969, 410.2900085449219, 412.8699951171875, 20745300, 0.83, 0.0),
(21, 'AAPL', '2024-11-21 00:00:00', 228.8800048828125, 230.16000366210938, 225.7100067138672, 228.52000427246094, 42071900, 0.00, 0.0),
(22, 'MSFT', '2024-11-21 00:00:00', 419.5, 419.7799987792969, 410.2900085449219, 412.8699951171875, 20745300, 0.83, 0.0),
(23, 'AAPL', '2024-11-21 00:00:00', 228.8800048828125, 230.16000366210938, 225.7100067138672, 228.52000427246094, 42071900, 0.00, 0.0),
(24, 'AAPL', '2024-11-21 00:00:00', 228.8800048828125, 230.16000366210938, 225.7100067138672, 228.52000427246094, 42071900, 0.00, 0.0),
(25, 'MSFT', '2024-11-21 00:00:00', 419.5, 419.7799987792969, 410.2900085449219, 412.8699951171875, 20745300, 0.83, 0.0),
(26, 'MSFT', '2024-11-21 00:00:00', 419.5, 419.7799987792969, 410.2900085449219, 412.8699951171875, 20745300, 0.83, 0.0),
(27, 'MSFT', '2024-11-21 00:00:00', 419.5, 419.7799987792969, 410.2900085449219, 412.8699951171875, 20745300, 0.83, 0.0),
(28, 'AAPL', '2024-11-21 00:00:00', 228.8800048828125, 230.16000366210938, 225.7100067138672, 228.52000427246094, 42071900, 0.00, 0.0),
(29, 'MSFT', '2024-11-21 00:00:00', 419.5, 419.7799987792969, 410.2900085449219, 412.8699951171875, 20745300, 0.83, 0.0),
(30, 'AAPL', '2024-11-21 00:00:00', 228.8800048828125, 230.16000366210938, 225.7100067138672, 228.52000427246094, 42071900, 0.00, 0.0),
(31, 'MSFT', '2024-11-22 00:00:00', 411.364990234375, 415.5950012207031, 411.05999755859375, 412.3299865722656, 6369080, 0.00, 0.0),
(32, 'AAPL', '2024-11-22 00:00:00', 228.05999755859375, 230.1300048828125, 228.05999755859375, 229.27000427246094, 10860296, 0.00, 0.0),
(33, 'MSFT', '2024-11-22 00:00:00', 411.364990234375, 415.5950012207031, 411.05999755859375, 412.2467956542969, 6404098, 0.00, 0.0),
(34, 'MSFT', '2024-11-22 00:00:00', 411.364990234375, 415.5950012207031, 411.05999755859375, 412.7644958496094, 7078925, 0.00, 0.0),
(35, 'MSFT', '2024-11-22 00:00:00', 411.364990234375, 415.5950012207031, 411.05999755859375, 412.7300109863281, 7320670, 0.00, 0.0),
(37, 'MSFT', '2024-11-22 00:00:00', 411.364990234375, 415.5950012207031, 411.05999755859375, 413.17999267578125, 7401463, 0.00, 0.0),
(38, 'AAPL', '2024-11-22 00:00:00', 228.05999755859375, 230.1300048828125, 228.05999755859375, 229.49000549316406, 11442589, 0.00, 0.0),
(39, 'MSFT', '2024-11-22 00:00:00', 411.364990234375, 415.5950012207031, 411.05999755859375, 412.864990234375, 7423794, 0.00, 0.0),
(40, 'AAPL', '2024-11-22 00:00:00', 228.05999755859375, 230.1300048828125, 228.05999755859375, 229.4499969482422, 11487440, 0.00, 0.0);

-- --------------------------------------------------------

--
-- Struttura della tabella `sessioni_utenti`
--

CREATE TABLE `sessioni_utenti` (
  `id` int(11) NOT NULL,
  `id_utente` int(11) NOT NULL,
  `id_dato` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dump dei dati per la tabella `sessioni_utenti`
--

INSERT INTO `sessioni_utenti` (`id`, `id_utente`, `id_dato`) VALUES
(73, 10, 37),
(74, 12, 38),
(75, 13, 38),
(76, 14, 37),
(77, 15, 37),
(78, 10, 39),
(79, 12, 40),
(80, 13, 40),
(81, 14, 39),
(82, 15, 39);

-- --------------------------------------------------------

--
-- Struttura della tabella `utenti`
--

CREATE TABLE `utenti` (
  `id` int(11) NOT NULL,
  `email` varchar(45) NOT NULL,
  `ticker` varchar(45) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dump dei dati per la tabella `utenti`
--

INSERT INTO `utenti` (`id`, `email`, `ticker`) VALUES
(10, 'signore.marco@hotmail.com', 'MSFT'),
(12, 'azz@gmail.com', 'AAPL'),
(13, 'fortran@yahoo.it', 'AAPL'),
(14, 'ejhdoekdoe@sss.d', 'MSFT'),
(15, 'aaa@bbb.it', 'MSFT');

--
-- Indici per le tabelle scaricate
--

--
-- Indici per le tabelle `dati`
--
ALTER TABLE `dati`
  ADD PRIMARY KEY (`id`);

--
-- Indici per le tabelle `sessioni_utenti`
--
ALTER TABLE `sessioni_utenti`
  ADD PRIMARY KEY (`id`),
  ADD KEY `FK_us` (`id_utente`),
  ADD KEY `FK_ds` (`id_dato`),
  ADD UNIQUE KEY `utente_dato` (`id_utente`,`id_dato`);

--
-- Indici per le tabelle `utenti`
--
ALTER TABLE `utenti`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `email` (`email`);

--
-- AUTO_INCREMENT per le tabelle scaricate
--

--
-- AUTO_INCREMENT per la tabella `dati`
--
ALTER TABLE `dati`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=41;

--
-- AUTO_INCREMENT per la tabella `sessioni_utenti`
--
ALTER TABLE `sessioni_utenti`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=83;

--
-- AUTO_INCREMENT per la tabella `utenti`
--
ALTER TABLE `utenti`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=16;

--
-- Limiti per le tabelle scaricate
--

--
-- Limiti per la tabella `sessioni_utenti`
--
ALTER TABLE `sessioni_utenti`
  ADD CONSTRAINT `FK_ds` FOREIGN KEY (`id_dato`) REFERENCES `dati` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `FK_us` FOREIGN KEY (`id_utente`) REFERENCES `utenti` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
