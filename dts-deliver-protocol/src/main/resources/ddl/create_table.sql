CREATE TABLE `dtstest`.`users` (
`id` int NOT NULL AUTO_INCREMENT,
`nickname` varchar(255) NOT NULL,
`deleted_at` timestamp NULL DEFAULT NULL,
`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`updated_at` timestamp NULL DEFAULT NULL,
`address` varchar(255) NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `unq_nick` (`nickname`),
KEY `ind_dd` (`address`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COMMENT 'All system users'