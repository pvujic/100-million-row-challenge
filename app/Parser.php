<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $fileSize = filesize($inputPath);
        $workerCount = 2;

        // Phase 1: Calculate chunk boundaries on newline boundaries
        $boundaries = $this->calculateBoundaries($inputPath, $fileSize, $workerCount);
        $actualWorkers = count($boundaries) - 1;

        // Phase 2: Fork children for chunks 1..N-1, parent processes chunk 0
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $pid = getmypid();
        $children = [];

        for ($w = 1; $w < $actualWorkers; $w++) {
            $childPid = pcntl_fork();
            if ($childPid === 0) {
                $childFlat = $this->parseChunk($inputPath, $boundaries[$w], $boundaries[$w + 1]);
                file_put_contents("$tmpDir/100m_{$pid}_$w", serialize($childFlat));
                exit(0);
            }
            $children[$w] = $childPid;
        }

        // Parent processes first chunk (preserves first-seen path order)
        $flat = $this->parseChunk($inputPath, $boundaries[0], $boundaries[1]);

        // Phase 3: Wait + merge children in order
        for ($w = 1; $w < $actualWorkers; $w++) {
            pcntl_waitpid($children[$w], $status);
            $childFlat = unserialize(file_get_contents("$tmpDir/100m_{$pid}_$w"));
            @unlink("$tmpDir/100m_{$pid}_$w");
            foreach ($childFlat as $key => $count) {
                $flat[$key] = ($flat[$key] ?? 0) + $count;
            }
            unset($childFlat);
        }

        // Phase 4: Split composite keys → nested array (preserving first-seen order)
        $data = [];
        foreach ($flat as $key => $count) {
            $path = substr($key, 0, -11);   // "/blog/slug"
            $date = substr($key, -10);       // "YYYY-MM-DD"
            $data[$path][$date] = $count;
        }
        unset($flat);

        // Phase 5: Sort dates + write output
        foreach ($data as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }

    private function calculateBoundaries(string $path, int $fileSize, int $workers): array
    {
        $boundaries = [0];
        $handle = fopen($path, 'rb');
        for ($i = 1; $i < $workers; $i++) {
            fseek($handle, (int)($fileSize * $i / $workers));
            fgets($handle); // advance past partial line
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;
        return $boundaries;
    }

    private function parseChunk(string $path, int $start, int $end): array
    {
        $flat = [];
        $handle = fopen($path, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;
        $leftover = '';
        set_error_handler(static function () { return true; });

        while ($remaining > 0) {
            $toRead = $remaining > 131072 ? 131072 : $remaining;
            $raw = fread($handle, $toRead);
            if ($raw === false || $raw === '') break;
            $remaining -= strlen($raw);

            if ($leftover !== '') {
                $raw = $leftover . $raw;
                $leftover = '';
            }

            $lines = explode("\n", $raw);
            $leftover = array_pop($lines);

            foreach ($lines as $line) {
                $flat[substr($line, 19, -15)]++;
            }
        }

        // Handle final leftover (line without trailing newline)
        if ($leftover !== '') {
            $flat[substr($leftover, 19, -15)]++;
        }

        restore_error_handler();
        fclose($handle);
        return $flat;
    }
}
