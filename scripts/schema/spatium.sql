-- phpMyAdmin SQL Dump
-- version 4.0.10deb1
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Apr 24, 2016 at 09:15 AM
-- Server version: 5.5.49-0ubuntu0.14.04.1
-- PHP Version: 5.5.9-1ubuntu4.16

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `spatium`
--

-- --------------------------------------------------------

--
-- Table structure for table `dataset`
--

CREATE TABLE IF NOT EXISTS `dataset` (
  `id` int(11) NOT NULL,
  `longitude` double(25,15) NOT NULL,
  `latitude` double(25,15) NOT NULL,
  `primary_type` varchar(100) NOT NULL,
  `date` int(11) NOT NULL,
  `x_coordinate` int(11) NOT NULL,
  `y_coordinate` int(11) NOT NULL,
  `district` int(11) NOT NULL,
  `ward` int(11) NOT NULL,
  `community_area` int(11) NOT NULL,
  `fbi_code` varchar(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `primary_type` (`primary_type`),
  KEY `date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
