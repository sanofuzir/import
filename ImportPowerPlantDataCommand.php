<?php

namespace AppBundle\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\Question;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Question\ConfirmationQuestion;
use Symfony\Component\Console\Input\InputOption;

/**
 * Class ImportOldDataCommand 
 * Command import old power plant data from MySQL database
 *
 * @method configure()
 * @method execute(InputInterface $input, OutputInterface $output)
 */
class ImportPowerPlantDataCommand extends ContainerAwareCommand
{
    /**
     * MySQL connection
     *
     * @var $connection
     */
    private $connection;

    /**
     * InfluxDB measurements
     *
     * @var array $measurements
     */
    private static $measurements = ['all_data', 'card_min', 'inverter_min', 'sensors', 'daily_energy', 'daily_energy_inverter'];

    /**
     * Standard influx template for point array
     *
     * @var $point
     */
    private static $point = [
        'value' => NULL,
        'tags' => [],
        'fields' => [],
        'time' => NULL
    ];

    protected function configure()
    {
        $this
            ->setName('powerplant:import:data')
            ->setDescription('Import power plant data from mysql tables')
            ->addArgument(
                'tech_name',
                InputArgument::REQUIRED,
                'Tech name of power plant (New one like in MOSE-WEB App)'
            )
            ->addArgument(
                'old_name',
                InputArgument::REQUIRED,
                'Old power plant name for mysql tables'
            )
            ->addArgument(
                'system',
                InputArgument::REQUIRED,
                'System (kaco, solarlog, sma, sma2)'
            )
            ->addOption(
                'centralInv',
                'ci',
                InputOption::VALUE_REQUIRED,
                'If power plant have central inverter, set the inverter like: SCAIT1AW:180210358'
            )
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        // Check if power plant exist
        $this->checkExistenceOfPP($input->getArgument('tech_name'));
        // Query data from mysql database
        $tables = $this->getTableNames($input->getArgument('old_name'), $input->getArgument('system'));
        // Display availible tables
        $output->writeln("\n<info>Power plant: ".$input->getArgument('tech_name')."\nAvailible tables are:</info>");
        foreach ($tables as $table) {
            $sql = "SELECT (data_length+index_length)/power(1024,2) tablesize_mb
                    FROM information_schema.tables
                    WHERE table_schema='monitor' and table_name='".$table."';";
            $size = number_format($this->runMySQLQuery($sql)[0]['tablesize_mb'], 2, ',', '.');
            $output->writeln("<info>Name: ".$table.", Size:".$size." Mb</info>");
        }
        // Ask for continue
        $helper = $this->getHelper('question');
        $question = new ConfirmationQuestion('Continue with this import? (y/n)', false);
        if (!$helper->ask($input, $output, $question)) {
            return;
        }
        // Start quering data for measurements and writing points into InfluxDB
        $output->writeln("\n<info>Starting to query data from old database and writing InfluxDb points...</info>");
        $numOfPoints = $this->startQueryAndWrite($input, $output, $tables, $input->getArgument('system'));
        $output->writeln("\n<info>Power plant: ".$input->getArgument('tech_name')." old data from MOSE MySQL database was imported into Influx database.".$numOfPoints." points was inserted!</info>");
    }

    /**
     * Check existence of Power plant
     *
     * @param string $techName
     * @return boolean
     */
    protected function checkExistenceOfPP($name)
    {
        $exist = FALSE;
        $powerPlants = $this->getContainer()->get('app.power_plant_manager')->findPowerPlants();
        foreach ($powerPlants as $key => $powerPlant) {
            if ($powerPlant->getTechName() == $name) {
                $exist = TRUE;
            }
        }
        if (!$exist) {
            throw new \Exception("Power plant does not exist!", 1);
            return FALSE;
        }

        return TRUE;
    }

    /**
     * Get MySQL connection and execute query
     *
     * @param string $sql
     * @return array (on success)
     */
    private function runMySQLQuery($sql)
    {
        // Get Mysql connection
        $this->connection = new \mysqli('192.168.xxx.xxx', 'user', 'pass', 'database', 3306);

        // Run query
        try {
            $data = $this->connection->query($sql)->fetch_all(MYSQLI_ASSOC);
        } catch (\Exception $e) {
            throw new \Exception("Error processing SQL query into MySQL database: ".$e->getMessage(), 1);
        }
        $this->connection->close();
                
        return $data;
    }

    /**
     * Get tablenames for power plant and system
     *
     * @param string $oldName
     * @param string $system
     * @return array
     */
    protected function getTableNames($oldName, $system)
    {
        $tables = [];
        switch ($system) {
            case 'kaco':
                $tables = [
                  'KACO_'.$oldName.'_ana',
                  'KACO_'.$oldName.'_energy',
                  'KACO_'.$oldName.'_kwr',
                  'KACO_'.$oldName.'_irr',
                  'KACO_'.$oldName.'_status',
                  'utility_'.$oldName
                ];
                break;
            
            case 'sma':
                $tables = [
                    'data_'.$oldName,
                    'energy_'.$oldName,
                    'irr_'.$oldName,
                    'live_'.$oldName,
                    'offset_'.$oldName,
                    'utility_'.$oldName
                ];
                break;

            case 'sma2':
                $tables = [
                    'SMA2_'.$oldName.'_data',
                    'SMA2_'.$oldName.'_energy',
                    'SMA2_'.$oldName.'_irr',
                    'SMA2_'.$oldName.'_live',
                    'SMA2_'.$oldName.'_offset',
                    '_utility'.$oldName
                ];
                break;

            case 'solarlog':
                $tables = [
                    'SOLARLOG_'.$oldName.'_devices',
                    'SOLARLOG_'.$oldName.'_energy',
                    'SOLARLOG_'.$oldName.'_events',
                    'SOLARLOG_'.$oldName.'_irr',
                    'SOLARLOG_'.$oldName.'_min',
                    'SOLARLOG_'.$oldName.'_sens',
                    'utility_'.$oldName,
                ];
                break;

            default:
                throw new \Exception("Error Processing Request - Unknown system!", 1);
                break;
        }

        return $this->checkIfTablesExist($tables);
    }

    /**
     * Check if listed tables exist in MySQL database
     *
     * @param array $tables
     * @return array
     */
    protected function checkIfTablesExist($tables)
    {
        $data = [];
        foreach ($tables as $key => $table) {
            $sql = "SHOW TABLES LIKE '".$table."';";
            $result = $this->runMySQLQuery($sql);
            if (!empty($result)) {
                array_push($data, $table);
            }
        }

        return $data;
    }

    /**
     * Get data from MySQL database
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @param Array $tables
     * @param string $system
     * @return integer
     */
    protected function startQueryAndWrite(InputInterface $input, OutputInterface $output, $tables, $system)
    {
        $numOfSenorPoints = $this->importSensorsData($input, $output, $tables, $system);
        $numOfInverterPoints = $this->importInvertersData($input, $output, $tables, $system);
        $numOfEnergyPoints = $this->importEnergyData($input, $output, $tables, $system);
        $numOfAllDataPoints = $this->importAllData($input, $output, $tables, $system);
        
        return $numOfSenorPoints + $numOfInverterPoints + $numOfEnergyPoints + $numOfAllDataPoints;
    }

    /**
     * Import all data measurement
     *
     * @param InputInterface $input
     * @param OutoutInterface $output
     * @param array $tables
     * @param string $system
     * @return int
     */
    protected function importAllData(InputInterface $input, OutputInterface $output, $tables, $system)
    {
        // Ask for continue
        $helper = $this->getHelper('question');
        $question = new ConfirmationQuestion('Continue with this importing all_data? (y/n)', false);
        if (!$helper->ask($input, $output, $question)) {
            return 0;
        }
        $output->writeln("<question>Start importing data all_data measurements</question>");
        // Find relevant tables per system
        $queryTable = NULL;
        switch ($system) {
            case 'kaco':
                foreach ($tables as $table) {
                    if (strpos($table, 'kwr') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            case 'sma':
                foreach ($tables as $table) {
                    if (strpos($table, 'data') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;
            
            case 'sma2':
                foreach ($tables as $table) {
                    if (strpos($table, 'data') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            case 'solarlog':
                foreach ($tables as $table) {
                    if (strpos($table, 'min') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            default:
                throw new \Exception("Unknown system!", 1);
                break;
        }

        // If no inverters data
        if (empty($queryTable)) {
            return 0;
        }
        // Get min and max date from table
        $output->writeln("<comment>Geting first and last date in table</comment>");
        if ($system == 'solarlog') {
            $sql = "SELECT min(`date_time`) AS 'min_date', max(`date_time`) AS 'max_date' FROM `".$queryTable."` WHERE `date_time` != '0000-00-00 00:00:00' AND `date_time` IS NOT NULL;";
        } elseif ($system == 'kaco') {
            $sql = "SELECT min(`Time`) AS 'min_date', max(`Time`) AS 'max_date' FROM `".$queryTable."` WHERE `Time` != '0000-00-00 00:00:00' AND `Time` IS NOT NULL;";
        } else {
            $sql = "SELECT min(`time`) AS 'min_date', max(`time`) AS 'max_date' FROM `".$queryTable."` WHERE `time` != '0000-00-00 00:00:00' AND `time` IS NOT NULL;";
        }
        $result = $this->runMySQLQuery($sql);
        if (empty($result)) {
            $output->writeln("<error>No dates find in table. Something is wrong with data in table...</error>");
            return FALSE;
        }
        $maxDate = new \DateTime($result[0]['max_date']);
        $minDate = new \DateTime($result[0]['min_date']);
        $output->writeln("<info>First availible date is ".$minDate->format('j.n.Y G:i:s')." and last availible date is ".$maxDate->format('j.n.Y G:i:s')." in table. Program will start importing data for one month at the time.</info>");
        
        // Start querys for each month
        $currentDateMin = $minDate;
        $month = 0;
        $numOfAllImportedPoints = 0;
        while ($currentDateMin < $maxDate) {
            $currentDateMin = new \DateTime($minDate->format("Y-m-d H:i:s"));
            $currentDateMin->modify('+ '.$month.' month');
            $currentDateMax = new \DateTime($currentDateMin->format("Y-m-d H:i:s"));
            $currentDateMax->modify('+ 1 month');
            $output->writeln("<comment>Start importing from ".$currentDateMin->format('j.n.Y')." until ".$currentDateMax->format('j.n.Y')."</comment>");
            if ($system == 'solarlog') {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `date_time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `date_time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            } elseif ($system == 'kaco') {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `Time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `Time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            } else {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            }
            $data = $this->runMySQLQuery($sql);
            $output->writeln("<comment>Converting ".count($data)." MySQL query inverters results into Influx points...</comment>");
            $points = $this->convertDataToInfluxPoints($input, $output, $data, $system, 'all_data');
            $output->writeln("\n<comment>Start writing ".count($points)." all_data points into Influx database...</comment>");
            if ($this->getContainer()->get('app.influx')->writePreparedPoints('all_data', $points) != FALSE) {
                $output->writeln("<info>".count($points)." all_data points writen to InfluxDB. Going to next month...</info>");
            } else {
                $output->writeln("<error>Error accrued while writing points to InfluxDB!</error>");
                return FALSE;
            }
            $numOfAllImportedPoints += count($points);
            $month +=1;
        }
        $output->writeln("<info>Finish with inverters data import!</info>");

        return $numOfAllImportedPoints;

    }

    /**
     * Query inverters data and import to influx
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @param Array $tables
     * @param string $system
     * @return int
     */
    protected function importInvertersData(InputInterface $input, OutputInterface $output, $tables, $system)
    {
        // Ask for continue
        $helper = $this->getHelper('question');
        $question = new ConfirmationQuestion('Continue with this importing inverters? (y/n)', false);
        if (!$helper->ask($input, $output, $question)) {
            return 0;
        }
        $output->writeln("<question>Start importing inverters data</question>");
        // Find relavant inverters table
        $queryTable = NULL;
        switch ($system) {
            case 'kaco':
                foreach ($tables as $table) {
                    if (strpos($table, 'kwr') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            case 'sma':
                foreach ($tables as $table) {
                    if (strpos($table, 'data') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;
            
            case 'sma2':
                foreach ($tables as $table) {
                    if (strpos($table, 'data') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            case 'solarlog':
                foreach ($tables as $table) {
                    if (strpos($table, 'min') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            default:
                throw new \Exception("Unknown system!", 1);
                break;
        }

        // If no inverters data
        if (empty($queryTable)) {
            return 0;
        }
        // Get min and max date from table
        $output->writeln("<comment>Geting first and last date in table</comment>");
        if ($system == 'solarlog') {
            $sql = "SELECT min(`date_time`) AS 'min_date', max(`date_time`) AS 'max_date' FROM `".$queryTable."` WHERE `date_time` != '0000-00-00 00:00:00' AND `date_time` IS NOT NULL;";
        } elseif ($system == 'kaco') {
            $sql = "SELECT min(`Time`) AS 'min_date', max(`Time`) AS 'max_date' FROM `".$queryTable."` WHERE `Time` != '0000-00-00 00:00:00' AND `Time` IS NOT NULL;";
        } else {
            $sql = "SELECT min(`time`) AS 'min_date', max(`time`) AS 'max_date' FROM `".$queryTable."` WHERE `time` != '0000-00-00 00:00:00' AND `time` IS NOT NULL;";
        }
        $result = $this->runMySQLQuery($sql);
        if (empty($result)) {
            $output->writeln("<error>No dates find in table. Something is wrong with data in table...</error>");
            return FALSE;
        }
        $maxDate = new \DateTime($result[0]['max_date']);
        $minDate = new \DateTime($result[0]['min_date']);
        $output->writeln("<info>First availible date is ".$minDate->format('j.n.Y G:i:s')." and last availible date is ".$maxDate->format('j.n.Y G:i:s')." in table. Program will start importing data for one month at the time.</info>");
        
        // Start querys for each month
        $currentDateMin = $minDate;
        $month = 0;
        $numOfAllImportedPoints = 0;
        while ($currentDateMin < $maxDate) {
            $currentDateMin = new \DateTime($minDate->format("Y-m-d H:i:s"));
            $currentDateMin->modify('+ '.$month.' month');
            $currentDateMax = new \DateTime($currentDateMin->format("Y-m-d H:i:s"));
            $currentDateMax->modify('+ 1 month');
            $output->writeln("<comment>Start importing from ".$currentDateMin->format('j.n.Y')." until ".$currentDateMax->format('j.n.Y')."</comment>");
            if ($system == 'solarlog') {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `date_time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `date_time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            } elseif ($system == 'kaco') {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `Time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `Time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            } else {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            }
            $data = $this->runMySQLQuery($sql);
            $output->writeln("<comment>Converting ".count($data)." MySQL query inverters results into Influx points...</comment>");
            $points = $this->convertDataToInfluxPoints($input, $output, $data, $system, 'inverters');
            if (!empty($input->getOption('centralInv'))) {
                // Inverter points
                $invPoints = $points['inverters_points'];
                // Card points
                $cardPoints = $points['cards_points'];
                $output->writeln("<comment>Start writing ".count($invPoints)+count($cardPoints)." into Influx database...</comment>");
                if ($this->getContainer()->get('app.influx')->writePreparedPoints('inverter_min', $invPoints) != FALSE) {
                    $output->writeln("<info>".count($invPoints)." inverter points writen to InfluxDB. Going to write card measurements...</info>");
                } else {
                    $output->writeln("<error>Error accrued while writing points to InfluxDB!</error>");
                }
                if ($this->getContainer()->get('app.influx')->writePreparedPoints('card_min', $cardPoints) != FALSE) {
                    $output->writeln("<info>".count($cardPoints)." card points writen to InfluxDB.</info>");
                } else {
                    $output->writeln("<error>Error accrued while writing points to InfluxDB!</error>");
                }
                $output->writeln("<info>".count($invPoints)+count($cardPoints)." points(card+inverters) writen to InfluxDB. Going to next month...</info>");
                $numOfAllImportedPoints += count($invPoints)+count($cardPoints);
            } else {
                $output->writeln("\n<comment>Start writing ".count($points)." inverters points into Influx database...</comment>");
                if ($this->getContainer()->get('app.influx')->writePreparedPoints('inverter_min', $points) != FALSE) {
                    $output->writeln("<info>".count($points)." points writen to InfluxDB. Going to next month...</info>");
                } else {
                    $output->writeln("<error>Error accrued while writing points to InfluxDB!</error>");
                    return FALSE;
                }
                $numOfAllImportedPoints += count($points);
            }
            $month +=1;
        }
        $output->writeln("<info>Finish with inverters data import!</info>");

        return $numOfAllImportedPoints;
    }

    /**
     * Query energy data and import to influx
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @param Array $tables
     * @param string $system
     * @return int
     */
    protected function importEnergyData(InputInterface $input, OutputInterface $output, $tables, $system)
    {
        // Ask for continue
        $helper = $this->getHelper('question');
        $question = new ConfirmationQuestion('Continue with this importing energy_data? (y/n)', false);
        if (!$helper->ask($input, $output, $question)) {
            return 0;
        }
        $output->writeln("<question>Start importing energy data</question>");
        // Find relavant energy table
        $queryTable = NULL;
        foreach ($tables as $table) {
            if (strpos($table, 'energy') !== FALSE) {
                $queryTable = $table;
            }
        }
        // If no energy data
        if (empty($queryTable)) {
            $output->writeln("<error>There is no energy table to import power plant yield data!</error>");
            return 0;
        }
        // Query data table from MySQL energy table (All at one, because it is small table)
        $sql = "SELECT * FROM `".$queryTable."`;";
        $data = $this->runMySQLQuery($sql);
        // Convert do InfluxDB points arrays
        $output->writeln("<comment>Converting ".count($data)." MySQL query energy results into Influx points...</comment>");
        $points = $this->convertDataToInfluxPoints($input, $output, $data, $system, 'energy');
        // Write daily energy measurements to Influx Database
        $output->writeln("<comment>Start writing ".count($points['daily_energy'])." daily energy points into Influx database...</comment>");
        if ($this->getContainer()->get('app.influx')->writePreparedPoints('daily_energy', $points['daily_energy']) != FALSE) {
            $output->writeln("<info>".count($points['daily_energy'])." points writed to InfluxDB.</info>");
        } else {
            $output->writeln("<error>Error accrued while writing points to InfluxDB!</error>");
            return FALSE;
        }
        // Write daily energy inverter measurements to Influx Database
        $output->writeln("<comment>Start writing ".count($points['daily_energy_inverter'])." daily energy inverter points into Influx database...</comment>");
        if ($this->getContainer()->get('app.influx')->writePreparedPoints('daily_energy_inverter', $points['daily_energy_inverter']) != FALSE) {
            $output->writeln("<info>".count($points['daily_energy_inverter'])." points writed to InfluxDB.</info>");
        } else {
            $output->writeln("<error>Error accrued while writing points to InfluxDB!</error>");
            return FALSE;
        }
        $output->writeln("<info>Finish with energy data import!".count($points['daily_energy']) + count($points['daily_energy_inverter'])." ponts writed!</info>");

        return count($points['daily_energy']) + count($points['daily_energy_inverter']);
    }

    /**
     * Query sensors data and import into influx
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @param Array $tables
     * @param string $system
     * @return int
     */
    protected function importSensorsData(InputInterface $input, OutputInterface $output, $tables, $system)
    {
        // Ask for continue
        $helper = $this->getHelper('question');
        $question = new ConfirmationQuestion('Continue with this importing sensors? (y/n)', false);
        if (!$helper->ask($input, $output, $question)) {
            return 0;
        }
        $output->writeln("<question>Start importing sensors data</question>");
        // Find relavant sensors table
        $queryTable = NULL;
        switch ($system) {
            case 'kaco':
                foreach ($tables as $table) {
                    if (strpos($table, 'ana') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            case 'sma':
                foreach ($tables as $table) {
                    if (strpos($table, 'data') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;
            
            case 'sma2':
                $output->writeln("<error>There is no sensors data on any sma2 power plant so we will break here and go on...</error>");
                return 0;
                break;

            case 'solarlog':
                foreach ($tables as $table) {
                    if (strpos($table, 'sens') !== FALSE) {
                        $queryTable = $table;
                    }
                }
                break;

            default:
                throw new \Exception("Unknown system!", 1);
                break;
        }
        // If no sensors data
        if (empty($queryTable)) {
            return 0;
        }
        // Get min and max date from table
        $output->writeln("<comment>Geting first and last date in table</comment>");
        if ($system == 'solarlog') {
            $sql = "SELECT min(`date_time`) AS 'min_date', max(`date_time`) AS 'max_date' FROM `".$queryTable."` WHERE `date_time` != '0000-00-00 00:00:00' AND `date_time` IS NOT NULL;";
        } elseif ($system == 'kaco') {
            $sql = "SELECT min(`Time`) AS 'min_date', max(`Time`) AS 'max_date' FROM `".$queryTable."` WHERE `Time` != '0000-00-00 00:00:00' AND `Time` IS NOT NULL;";
        } else {
            $sql = "SELECT min(`time`) AS 'min_date', max(`time`) AS 'max_date' FROM `".$queryTable."` WHERE `time` != '0000-00-00 00:00:00' AND `time` IS NOT NULL;";
        }
        $result = $this->runMySQLQuery($sql);
        if (empty($result)) {
            $output->writeln("<error>No dates find in table. Something is wrong with data in table...</error>");
            return FALSE;
        }
        $maxDate = new \DateTime($result[0]['max_date']);
        $minDate = new \DateTime($result[0]['min_date']);
        $output->writeln("<info>First availible date is ".$minDate->format('j.n.Y G:i:s')." and last availible date is ".$maxDate->format('j.n.Y G:i:s')." in table. Program will start importing data for one month at the time.</info>");
        
        // Start querys for each month
        $currentDateMin = $minDate;
        $month = 0;
        $numOfAllImportedPoints = 0;
        while ($currentDateMin < $maxDate) {
            $currentDateMin = new \DateTime($minDate->format("Y-m-d H:i:s"));
            $currentDateMin->modify('+ '.$month.' month');
            $currentDateMax = new \DateTime($currentDateMin->format("Y-m-d H:i:s"));
            $currentDateMax->modify('+ 1 month');
            $output->writeln("<comment>Start importing from ".$currentDateMin->format('j.n.Y')." until ".$currentDateMax->format('j.n.Y')."</comment>");
            if ($system == 'solarlog') {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `date_time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `date_time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            } elseif ($system == 'kaco') {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `Time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `Time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            } else {
                $sql = "SELECT * FROM `".$queryTable."` WHERE `time` > '".$currentDateMin->format('Y-m-d H:i:s')."' AND `time` < '".$currentDateMax->format('Y-m-d H:i:s')."';";
            }
            $data = $this->runMySQLQuery($sql);
            $output->writeln("<comment>Converting ".count($data)." MySQL query sensors results into Influx points...</comment>");
            $points = $this->convertDataToInfluxPoints($input, $output, $data, $system, 'sensors');
            $output->writeln("\n<comment>Start writing ".count($points)." sensors points into Influx database...</comment>");
            if ($this->getContainer()->get('app.influx')->writePreparedPoints('sensors', $points) != FALSE) {
                $output->writeln("<info>".count($points)." points writen to InfluxDB. Going to next month...</info>");
            } else {
                $output->writeln("<error>Error accrued while writing points to InfluxDB!</error>");
                return FALSE;
            }
            $month +=1;
            $numOfAllImportedPoints += count($points);
        }
        $output->writeln("<info>Finish with sensors data import!</info>");

        return $numOfAllImportedPoints;
    }

    /**
     * Convert array to influx points
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @param array $data
     * @param string $system
     * @param string $type
     * @return array
     */
    protected function convertDataToInfluxPoints(InputInterface $input, OutputInterface $output, $data, $system, $type)
    {
        $points = [];
        switch ($type) {
            case 'sensors':
                if ($system == 'kaco') {
                    //Inicialization of progress bar
                    $progress = new ProgressBar($output, count($data));
                    $progress->start();
                    foreach ($data as $row) {
                        $point = [
                            'value' => NULL,
                            'tags' => [
                                'power_plant' => $input->getArgument('tech_name'),
                                'device' => 'sensorbox',
                                ],
                            'fields' => [
                                'sol_irr' => (float) $row['G_M0'],
                                'temp_amb' => (float) $row['T_U0'],
                                'temp_mod' => (float) $row['T_M0']
                                ],
                            'time' => $row['Time']
                        ];
                        $point['fields']['wind'] = isset($row['W_V0']) ? (float) $row['W_V0'] : (float) 0;
                        foreach ($point['fields'] as $key => $value) {
                            if ($value == NULL || empty($value)) {
                                $point['fields'][$key] = (float) 0;
                            }
                        }
                        array_push($points, $point);
                        $progress->advance(1);
                    }
                    $progress->finish();
                } elseif ($system == 'sma') {
                    // First convert array to have all measurements for the same time in one array under time
                    $dataPerTime = [];
                    //Inicialization of progress bar
                    $progress = new ProgressBar($output, count($data));
                    $progress->start();
                    foreach ($data as $row) {
                        if (strpos($row['dev_id'], 'SENS') !== FALSE) {
                            $dataPerTime[$row['time']][] = $row;
                        }
                    }
                    // Get data from arrays
                    foreach ($dataPerTime as $time => $rows) {
                        $sol_irr = (float) 0;
                        $temp_amb = (float) 0;
                        $temp_mod = (float) 0;
                        $wind = (float) 0;
                        foreach ($rows as $row) {
                            if ($row['dev_param'] == 'IntSolIrr') {
                                $sol_irr = (float) $row['avg_val'];
                            } elseif ($row['dev_param'] == 'TmpAmb C') {
                                $temp_amb = (float) $row['avg_val'];
                            } elseif ($row['dev_param'] == 'TmpMdul C') {
                                $temp_mod = (float) $row['avg_val'];
                            } elseif ($row['dev_param'] == 'WindVel m/s') {
                                $wind = (float) $row['avg_val'];
                            }
                            
                        }
                        $point = [
                            'value' => NULL,
                            'tags' => [
                                'power_plant' => $input->getArgument('tech_name'),
                                'device' => 'sensorbox',
                                ],
                            'fields' => [
                                'sol_irr' => $sol_irr,
                                'temp_amb' => $temp_amb,
                                'temp_mod' => $temp_mod,
                                'wind' => $wind
                                ],
                            'time' => $time
                        ];
                        foreach ($point['fields'] as $key => $value) {
                            if ($value == NULL || empty($value)) {
                                $point['fields'][$key] = (float) 0;
                            }
                        }
                        array_push($points, $point);
                        $progress->advance(1);
                    }
                    $progress->finish();
                } elseif ($system == 'sma2') {
                    // no sensors on any sma2 power plant so break...
                    $output->writeln("<error>There is no sensors data on any sma2 power plant so we will break here and go on...</error>");
                    break;
                } elseif ($system == 'solarlog') {
                    //Inicialization of progress bar
                    $progress = new ProgressBar($output, count($data));
                    $progress->start();
                    foreach ($data as $row) {
                        $point = [
                            'value' => NULL,
                            'tags' => [
                                'power_plant' => $input->getArgument('tech_name'),
                                'device' => 'sensorbox',
                                ],
                            'fields' => [
                                'sol_irr' => (float) $row['irradiation'],
                                'temp_amb' => (float) $row['ambient_temp'],
                                'temp_mod' => (float) $row['module_temp'],
                                'wind' => (float) $row['wind']
                                ],
                            'time' => $row['date_time']
                        ];
                        foreach ($point['fields'] as $key => $value) {
                            if ($value == NULL || empty($value)) {
                                $point['fields'][$key] = (float) 0;
                            }
                        }
                        array_push($points, $point);
                        $progress->advance(1);
                    }
                    $progress->finish();   
                }
                
                break;
            
            case 'energy':
                $dailyEnergy = [];
                $dailyInv = [];
                foreach ($data as $row) {
                    $point = self::$point;
                    $point['value'] = (float) $row['energy'];
                    $point['tags'] = ['power_plant' => $input->getArgument('tech_name')];
                    if ($system == 'kaco') {
                        $point['time'] = $row['Date'];
                    } else {
                        $point['time'] = $row['date'];    
                    }
                    array_push($dailyEnergy, $point);
                }
                foreach ($data as $row) {
                    $point = self::$point;
                    $point['value'] = (float) $row['energy'];
                    $point['tags'] = ['power_plant' => $input->getArgument('tech_name'), 'device' => $row['dev_id']];
                    if ($system == 'kaco') {
                        $point['time'] = $row['Date'];
                    } else {
                        $point['time'] = $row['date'];    
                    }
                    array_push($dailyInv, $point);
                }
                $points['daily_energy'] = $dailyEnergy;
                $points['daily_energy_inverter'] = $dailyInv;
                break;

            case 'inverters':
                switch ($system) {
                    case 'kaco':
                        //Inicialization of progress bar
                        $progress = new ProgressBar($output, count($data));
                        $progress->start();
                        foreach ($data as $row) {
                            $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $row['Adresse']);
                            $status = $this->findStatusForDevice($input, $system, $row['Adresse'], $row['Time'], 'status');
                            $error = $this->findStatusForDevice($input, $system, $row['Adresse'], $row['Time'], 'error');
                            $point = [
                                'value' => NULL,
                                'tags' => [
                                    'power_plant' => $input->getArgument('tech_name'),
                                    'device' => $row['Adresse'],
                                    'logger_ip' => $loggerIp,
                                    ],
                                'fields' => [
                                    'type' => 'string_inv',
                                    'e_total' => (float) $row['E_D_WR'],
                                    'status' => (string) $status,
                                    'error' => (string) $error,
                                    'temp' => (float) $row['T_C'],
                                    'pac' => (float) $row['P_AC_WR'],
                                    'uac' => (float) $row['U_AC_0'],
                                    'iac' => (float) $row['I_AC_0'],
                                    'pdc1' => (float) $row['P_DC_1'],
                                    'udc1' => (float) $row['U_DC_1'],
                                    'idc1' => (float) $row['I_DC_1'],
                                    'pdc2' => (float) $row['P_DC_2'],
                                    'udc2' => (float) $row['U_DC_2'],
                                    'idc2' => (float) $row['I_DC_2'],
                                    'pdc3' => (float) $row['P_DC_3'],
                                    'udc3' => (float) $row['U_DC_3'],
                                    'idc3' => (float) $row['I_DC_3']
                                    ],
                                'time' => $row['Time']
                            ];
                            foreach ($point['fields'] as $key => $value) {
                                if ($key == 'status' && empty($value)) {
                                    $point['fields']['status'] = (string) '';
                                } elseif ($key == 'error' && empty($value)) {
                                    $point['fields']['error'] = (string) '';
                                } elseif ($value == NULL || empty($value)) {
                                    $point['fields'][$key] = (float) 0;
                                }
                            }
                            array_push($points, $point);
                            $progress->advance(1);
                        }
                        $progress->finish();
                        break;

                    case 'sma':
                        // First convert array to have all measurements for the same time in one array under time
                        $points = [];
                        $dataPerTime = [];
                        foreach ($data as $row) {
                            if (strpos($row['dev_id'], 'SENS') === FALSE) {
                                $dataPerTime[$row['time']][] = $row;
                            }
                        }
                        // Fillup variables
                        if (!empty($input->getOption('centralInv'))) {
                            $points = $this->createCentralInvPoints($input, $dataPerTime);
                        } else {
                            //Inicialization of progress bar
                            $progress = new ProgressBar($output, count($dataPerTime));
                            $progress->start();
                            foreach ($dataPerTime as $time => $rows) {
                                $pdc1 = $pdc2 = $idc1 = $idc2 = $udc1 = $udc2 = 0;
                                foreach ($rows as $row) {
                                    if ($row['dev_param'] == 'Serial Number') {
                                        $device = (float) $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'E-Total') {
                                        $energy = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'Op.GriSwStt') {
                                        $status = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'Op.EvtNo') {
                                        $error = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'Pac') {
                                        $pac = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'Uac' || $row['dev_param'] == 'GridMs.PhV.phsA') {
                                        $uac = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'GridMs.A.phsA' || $row['dev_param'] == 'Iac-Ist' || $row['dev_param'] == 'Iac') {
                                        $iac = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'A.Ms.Watt') {
                                        $pdc1 = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'B.Ms.Watt') {
                                        $pdc2 = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'A.Ms.Amp' || $row['dev_param'] == 'Ipv') {
                                        $idc1 = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'B.Ms.Amp') {
                                        $idc2 = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'A.Ms.Vol' || $row['dev_param'] == 'Upv-Ist') {
                                        $udc1 = $row['avg_val'];
                                    } elseif ($row['dev_param'] == 'B.Ms.Vol') {
                                        $udc2 = $row['avg_val'];
                                    }
                                }
                                if (empty($pdc1) || empty($pdc2)) {
                                    $pdc1 = $udc1 * $idc1;
                                    $pdc2 = $udc2 * $idc2;
                                }
                                if (empty($device)) {
                                    $peaces = explode(':', $rows[0]['dev_id']);
                                    $device = $peaces[1];
                                }
                                if (empty($status)) {
                                    $status = '0';
                                }
                                if (empty($error)) {
                                    $error = '0';
                                }
                                $sql = "SELECT `avg_val` FROM data_".$input->getArgument('old_name')." WHERE `dev_param`='E-total' AND date(`time`)=date('".$time."') AND `dev_id` LIKE '%".$device."' ORDER BY `time` ASC LIMIT 1;";
                                $offset = $this->runMySQLQuery($sql);
                                $energy -= $offset[0]['avg_val'];
                                $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $device);
                                $point = [
                                    'value' => NULL,
                                    'tags' => [
                                        'power_plant' => $input->getArgument('tech_name'),
                                        'device' => $device,
                                        'logger_ip' => $loggerIp,
                                        ],
                                    'fields' => [
                                        'type' => 'string_inv',
                                        'e_total' => (float) $energy,
                                        'status' => (string) $status,
                                        'error' => (string) $error,
                                        'temp' => (float) 0,
                                        'pac' => (float) $pac,
                                        'uac' => (float) $uac,
                                        'iac' => (float) $iac,
                                        'pdc1' => (float) $pdc1,
                                        'udc1' => (float) $udc1,
                                        'idc1' => (float) $idc1,
                                        'pdc2' => (float) $pdc2,
                                        'udc2' => (float) $udc2,
                                        'idc2' => (float) $idc2,
                                        'pdc3' => (float) 0,
                                        'udc3' => (float) 0,
                                        'idc3' => (float) 0
                                        ],
                                    'time' => $row['time']
                                ];
                                foreach ($point['fields'] as $key => $value) {
                                if ($key == 'status' && empty($value)) {
                                    $point['fields']['status'] = (string) '';
                                } elseif ($key == 'error' && empty($value)) {
                                    $point['fields']['error'] = (string) '';
                                } elseif ($value == NULL || empty($value)) {
                                    $point['fields'][$key] = (float) 0;
                                }
                            }
                                array_push($points, $point);
                                $progress->advance(1);
                            }
                            $progress->finish();
                        }
                        break;

                    case 'sma2':
                        // First convert array to have all measurements for the same time in one array under time
                        $dataPerTime = [];
                        foreach ($data as $row) {
                            if (strpos($row['dev_id'], 'WebBox') === FALSE) {
                                $dataPerTime[$row['time']][] = $row;
                            }
                        }
                        // Fillup variables
                        $devices = [];
                        foreach ($dataPerTime as $rows) {
                            foreach ($rows as $row) {
                                if (array_search($row['dev_id'], $devices) === FALSE) {
                                    array_push($devices, $row['dev_id']);
                                }
                            }
                            break;
                        }
                        $points = [];
                        //Inicialization of progress bar
                        $progress = new ProgressBar($output, count($dataPerTime));
                        $progress->start();
                        // Create measurements points from dev param and device array
                        foreach ($dataPerTime as $time => $rows) {
                            foreach ($devices as $device) {
                                // Create array of measuremnts foreach device for this time
                                foreach ($rows as $row) {
                                    if ($device == $row['dev_id']) {
                                        $uac = 0;
                                        $iac = 0;
                                        if ($row['dev_param'] == 'Metering.TotWhOut') {
                                            $sql = "SELECT `avg_val` FROM SMA2_".$input->getArgument('old_name')."_data WHERE `dev_param`='Metering.TotWhOut' AND `time`='".$time."' AND `dev_id`='".$device."' ORDER BY `time` ASC LIMIT 1;";
                                            $offset = $this->runMySQLQuery($sql);
                                            $energy = $row['avg_val'] - $offset[0]['avg_val'];
                                        } elseif ($row['dev_param'] == 'Operation.Health') {
                                            $status = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'Operation.Evt.Msg') {
                                            $error = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'GridMs.TotW') {
                                            $pac = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'GridMs.PhV.phsA' || $row['dev_param'] == 'GridMs.PhV.phsB') {
                                            $uac += $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'GridMs.A.phsA' || $row['dev_param'] == 'GridMs.A.phsB') {
                                            $iac += $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'DcMs.Watt[A]') {
                                            $pdc1 = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'DcMs.Watt[B]') {
                                            $pdc2 = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'DcMs.Amp[A]') {
                                            $idc1 = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'DcMs.Amp[B]') {
                                            $idc2 = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'DcMs.Vol[A]') {
                                            $udc1 = $row['avg_val'];
                                        } elseif ($row['dev_param'] == 'DcMs.Vol[B]') {
                                            $udc2 = $row['avg_val'];
                                        }
                                    }
                                }
                                // All measurements are set for this time and this device. Save point and go on next device
                                $serialNum = explode('SN:', $row['dev_id'])[1];
                                $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $serialNum);
                                $point = [
                                    'value' => NULL,
                                    'tags' => [
                                        'power_plant' => $input->getArgument('tech_name'),
                                        'device' => $serialNum,
                                        'logger_ip' => $loggerIp,
                                        ],
                                    'fields' => [
                                        'type' => 'string_inv',
                                        'e_total' => (float) $energy,
                                        'status' => (string) $status,
                                        'error' => (string) $error,
                                        'temp' => (float) 0,
                                        'pac' => (float) $pac,
                                        'uac' => (float) $uac,
                                        'iac' => (float) $iac,
                                        'pdc1' => (float) $pdc1,
                                        'udc1' => (float) $udc1,
                                        'idc1' => (float) $idc1,
                                        'pdc2' => (float) $pdc2,
                                        'udc2' => (float) $udc2,
                                        'idc2' => (float) $idc2,
                                        'pdc3' => (float) 0,
                                        'udc3' => (float) 0,
                                        'idc3' => (float) 0
                                        ],
                                    'time' => $row['time']
                                ];
                                foreach ($point['fields'] as $key => $value) {
                                    if ($key == 'status' && empty($value)) {
                                        $point['fields']['status'] = (string) 0;
                                    } elseif ($key == 'error' && empty($value)) {
                                        $point['fields']['error'] = (string) 0;
                                    } elseif ($value == NULL || empty($value)) {
                                        $point['fields'][$key] = (float) 0;
                                    }
                                }
                                array_push($points, $point);
                                $progress->advance(1);
                            }
                            $progress->finish();
                        }
                        break;

                    case 'solarlog':
                        $points = [];
                        //Inicialization of progress bar
                        $progress = new ProgressBar($output, count($data));
                        $progress->start();
                        foreach ($data as $row) {
                            $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $row['address']);
                            $status = $this->findStatusForDevice($input, $system, $row['address'], $row['date_time'], 'status');
                            $error = $this->findStatusForDevice($input, $system, $row['address'], $row['date_time'], 'error');
                            $point = [
                                'value' => NULL,
                                'tags' => [
                                    'power_plant' => $input->getArgument('tech_name'),
                                    'device' => $row['address'],
                                    'logger_ip' => $loggerIp,
                                    ],
                                'fields' => [
                                    'type' => 'string_inv',
                                    'e_total' => (float) $row['day_energy'],
                                    'status' => (string) $status,
                                    'error' => (string) $error,
                                    'temp' => (float) $row['Temp'],
                                    'pac' => (float) $row['PAC'],
                                    'uac' => (float) 0,
                                    'iac' => (float) 0,
                                    'pdc1' => (float) $row['PDC_S1'],
                                    'udc1' => (float) $row['UDC_S1'],
                                    'idc1' => (float) 0,
                                    'pdc2' => (float) $row['PDC_S2'],
                                    'udc2' => (float) $row['UDC_S2'],
                                    'idc2' => (float) 0,
                                    'pdc3' => (float) $row['PDC_S3'],
                                    'udc3' => (float) $row['UDC_S3'],
                                    'idc3' => (float) 0
                                    ],
                                'time' => $row['date_time']
                            ];
                            foreach ($point['fields'] as $key => $value) {
                                if ($key == 'status' && empty($value)) {
                                    $point['fields']['status'] = (string) '';
                                } elseif ($key == 'error' && empty($value)) {
                                    $point['fields']['error'] = (string) '';
                                } elseif ($value == NULL || empty($value)) {
                                    $point['fields'][$key] = (float) 0;
                                }
                            }
                            array_push($points, $point);
                            $progress->advance(1);
                        }
                        $progress->finish();
                        break;
                    
                    default:
                        throw new \Exception("Error Processing Request. Unknown system!", 1);
                        break;
                }
                break;
            
            case 'all_data':
                if ($system == 'sma') {
                    $points = [];
                    $dataPerTime = [];
                    foreach ($data as $row) {
                        if (strpos($row['dev_id'], 'SENS') === FALSE) {
                            $dataPerTime[$row['time']][] = $row;
                        }
                    }
                    //Inicialization of progress bar
                    $progress = new ProgressBar($output, count($dataPerTime));
                    $progress->start();
                    foreach ($dataPerTime as $time => $rows) {
                        $point = self::$point;
                        foreach ($rows as $key => $row) {
                            if ($row['dev_param'] == 'Serial Number') {
                                $dev = (float) $row['avg_val'];
                            } else {
                                $point['fields'][$key] = $row['avg_val'];
                            }
                        }
                        $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $dev);
                        $point['tags']['power_plant'] = $input->getArgument('tech_name');
                        $point['tags']['device'] = $dev;
                        $point['tags']['logger_ip'] = $loggerIp;
                        $point['time'] = $time;
                        array_push($points, $point);
                        $progress->advance(1);
                    }
                    $progress->finish();
                } elseif ($system == 'sma2') {
                    $dataPerTime = [];
                    foreach ($data as $row) {
                        if (strpos($row['dev_id'], 'WebBox') === FALSE) {
                            $dataPerTime[$row['time']][] = $row;
                        }
                    }
                    // Fillup variables
                    $devices = [];
                    foreach ($dataPerTime as $rows) {
                        foreach ($rows as $row) {
                            if (array_search($row['dev_id'], $devices) === FALSE) {
                                array_push($devices, $row['dev_id']);
                            }
                        }
                        break;
                    }
                    $points = [];
                    //Inicialization of progress bar
                    $progress = new ProgressBar($output, count($dataPerTime));
                    $progress->start();
                    foreach ($dataPerTime as $time => $rows) {
                        foreach ($devices as $device) {
                            $point = self::$point;
                            foreach ($rows as $row) {
                                if ($device == $row['dev_id']) {
                                    $point['fields'][$row['dev_param']] = $row['avg_val'];
                                }
                            }
                            $serialNum = explode('SN:', $row['dev_id'])[1];
                            $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $serialNum);
                            $point['tags']['power_plant'] = $input->getArgument('tech_name');
                            $point['tags']['device'] = $serialNum;
                            $point['tags']['logger_ip'] = $loggerIp;
                            $point['time'] = $time;
                            array_push($points, $point);
                            $progress->advance(1);
                        }
                    }
                    $progress->finish();
                } else {
                    $points = [];
                    //Inicialization of progress bar
                    $progress = new ProgressBar($output, count($data));
                    $progress->start();
                    foreach ($data as $row) {
                        if (isset($row['device'])) {
                            $dev = $row['device'];
                        }
                        if (isset($row['Adresse'])) {
                            $dev = $row['Adresse'];
                        }
                        if (isset($row['INV'])) {
                            $dev = $row['INV'];
                        }
                        if (isset($row['address'])) {
                            $dev = $row['address'];
                        }
                        $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $dev);
                        if ($dev != 'sensorbox') {
                            $point = self::$point;
                            $point['tags']['power_plant'] = $input->getArgument('tech_name');
                            $point['tags']['device'] = $dev;
                            $point['tags']['logger_ip'] = $loggerIp;
                            foreach ($row as $key => $field) {
                                if ($key != 'ID' || $key != 'id') {
                                    if(is_string($field) && strlen($field) > 1) {
                                        if ($system == 'sma2' || !empty($input->getOption('centralInv'))) {
                                            $point['fields'][$key] = !empty($field) ? (string) $field : (string) '';
                                        }
                                    } else {
                                        $point['fields'][$key] = !empty($field) ? (float) $field : (float) 0;
                                    }
                                }
                            }
                            // Time
                            if (isset($row['Time'])) {
                                $time = $row['Time'];
                            } elseif (isset($row['Uhrzeit'])) {
                                $time = $row['Uhrzeit'];
                            } elseif (isset($row['TimeStamp'])) {
                                $time = $row['TimeStamp'];
                            } elseif (isset($row['time'])) {
                                $time = $row['time'];
                            } elseif (isset($row['date_time'])) {
                                $time = $row['date_time'];
                            }
                            $point['time'] = $time;
                            array_push($points, $point);
                            $progress->advance(1);
                        }
                    }
                    $progress->finish();
                }
                
                break;
            default:
                throw new \Exception("Error Processing Request. Unknown type!", 1);
                break;
        }

        return $points;
    }

    /**
     * Create points for central inverter
     *
     * @param InputInterface $input
     * @param array $data
     * @return array $points
     */
    protected function createCentralInvPoints(InputInterface $input, $dataPerTime)
    {
        $points = [];
        $cards = [];
        foreach ($dataPerTime as $time => $rows) {
            foreach ($rows as $row) {
                if ($row['dev_id'] == $input->getOption('centralInv')) {
                    // Create points for inverter
                    $loggerIp = $this->getContainer()->get('app.webapp.mysql_interface.query_manager')->findLoggerIpForDevice($input->getArgument('tech_name'), $row['dev_id']);
                    if ($row['dev_param'] == 'E-heute') {
                        $energy = $row['avg_val'];
                    } elseif ($row['dev_param'] == 'Mode') {
                        $status = $row['avg_val'];
                    } elseif ($row['dev_param'] == 'Error') {
                        $error = $row['avg_val'];
                    } elseif ($row['dev_param'] == 'Pac') {
                        $pac = $row['avg_val'];
                    } elseif ($row['dev_param'] == 'Vpv') {
                        $uac = $row['avg_val'];
                    } elseif ($row['dev_param'] == 'Iac') {
                        $iac = $row['avg_val'];
                    } elseif ($row['dev_param'] == 'Iac') {
                        $iac = $row['avg_val'];
                    }
                    $point = [
                        'value' => NULL,
                        'tags' => [
                            'power_plant' => $input->getArgument('tech_name'),
                            'device' => $row['dev_id'],
                            'logger_ip' => $loggerIp,
                            ],
                        'fields' => [
                            'type' => 'central_inv',
                            'e_total' => $energy,
                            'status' => $status,
                            'error' => $error,
                            'temp' => NULL,
                            'pac' => $pac,
                            'uac' => $uac,
                            'iac' => $iac,
                            'pdc1' => NULL,
                            'udc1' => NULL,
                            'idc1' => NULL,
                            'pdc2' => NULL,
                            'udc2' => NULL,
                            'idc2' => NULL,
                            'pdc3' => NULL,
                            'udc3' => NULL,
                            'idc3' => NULL
                            ],
                        'time' => $row['Time']
                    ];
                    array_push($points, $point);
                    
                } else {
                    // Create point for cards
                    if (strpos($row['dev_id'], 'SCS') !== FALSE) {
                        // Controller
                        $type = 'controller';
                        if ($row['dev_param'] == 'Mode') {
                            $status = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'Error') {
                            $error = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'MeanCurGr1') {
                            $igrid = $row['avg_val'];
                        }
                    } else {
                        // Measurement cards
                        $type = 'card';
                        if ($row['dev_param'] == 'Status') {
                            $status = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'Fehler') {
                            $error = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 1') {
                            $idc1 = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 2') {
                            $idc2 = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 3') {
                            $idc3 = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 4') {
                            $idc4 = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 5') {
                            $idc5 = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 6') {
                            $idc6 = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 7') {
                            $idc7 = $row['avg_val'];
                        } elseif ($row['dev_param'] == 'IString 8') {
                            $idc8 = $row['avg_val'];
                        }
                    }
                    $card = [
                        'value' => NULL,
                        'tags' => [
                            'power_plant' => $input->getArgument('tech_name'),
                            'device' => $row['dev_id'],
                            'father' => $input->getOption('centralInv'),
                            ],
                        'fields' => [
                            'type' => $type,
                            'status' => $status,
                            'error' => $error,
                            'igrid' => $igrid,
                            'idc1' => $idc1,
                            'idc2' => $idc2,
                            'idc3' => $idc3,
                            'idc4' => $idc4,
                            'idc5' => $idc5,
                            'idc6' => $idc6,
                            'idc7' => $idc7,
                            'idc8' => $idc8
                            ],
                        'time' => $time
                    ];
                    array_push($cards, $card);
                }
            }
        }

        return ['inverter_points' => $points, 'cards_points' => $cards];
    }

    /**
     * Find status for device at this time
     *
     * @param InputInterface $input
     * @param string $system
     * @param string $device
     * @param string $time
     * @param string $type
     * @return string
     */
    protected function findStatusForDevice(InputInterface $input, $system, $device, $time, $type)
    {
        $status = NULL;
        $error = NULL;
        if ($system == 'solarlog') {
            $eventsTable = 'SOLARLOG_'.$input->getArgument('old_name').'_events';
            $sql = "SELECT `status`, `fehler` FROM `".$eventsTable."` WHERE `dev_id`='".$device."' AND `date_from` < '".$time."' AND `date_to` > '".$time."' LIMIT 1;";
            $data = $this->runMySQLQuery($sql);
            $status = current($data)['status'];
            $error = current($data)['fehler'];
        } elseif ($system == 'kaco') {
            $eventsTable = 'KACO_'.$input->getArgument('old_name').'_status';
            $sql = "SELECT `S_OLD` FROM `".$eventsTable."` WHERE `Addr`='".$device."' AND `Time` > '".$time."' ORDER BY `Time` ASC LIMIT 1;";
            $data = $this->runMySQLQuery($sql);
            $status = current($data)['S_OLD'];
            $error = $status;
        } else {
            // SMA and SMA2 systems have statuses and errors in data table
            return FALSE;
        }


        if ($type == 'status') {
            return $status;
        } else {
            return $error;
        }
    }

}
