import express, { Request, Response } from 'express';
import { Pool } from 'pg';

// Set up the Express app
const app = express();
const port = 3000;

// Set up the PostgreSQL connection pool
// const pool = new Pool({
//   user: '',
//   host: 'localhost',
//   database: 'test_postgis_db_for_water_wells',
//   password: '',
//   port: 5432,
// });

const fs = require('fs');

// Micro database
// const pool = new Pool({
//     user: 'postgres',
//     host: 'well-water-visualization-db-postgis-1.cn2uii4ioy0n.us-east-2.rds.amazonaws.com',
//     database: 'postgres',
//     password: 'FtJ?bO?IG#cj]1zuk38_}L+]d$xo',
//     port: 5432,
//     ssl: {
//         rejectUnauthorized: false, // You can set this to true and provide CA cert for better security
//         ca: fs.readFileSync('us-east-2-bundle.pem').toString(), // Path to the downloaded RDS root certificate
//     },
//   });

// Dev database
// const pool = new Pool({
//     user: 'postgres',
//     host: 'database-2.cn2uii4ioy0n.us-east-2.rds.amazonaws.com',
//     database: 'postgres',
//     password: '*c[:DqqjiGxm9mU-lYgp(47*?rL_',
//     port: 5432,
//     ssl: {
//         rejectUnauthorized: false, // You can set this to true and provide CA cert for better security
//         ca: fs.readFileSync('us-east-2-bundle.pem').toString(), // Path to the downloaded RDS root certificate
//     },
//   });

// Other dev data but a little closer
// HOST = "database-3-dev-n-california.cdu80o26o3iu.us-west-1.rds.amazonaws.com"
// DATABASE = "postgres"
// USER = "postgres"
// PASSWORD = "]URiwwLCZq80aaJsE:gRT0~y$zDc"
const pool = new Pool({
    user: 'postgres',
    host: 'database-3-dev-n-california.cdu80o26o3iu.us-west-1.rds.amazonaws.com',
    database: 'postgres',
    password: ']URiwwLCZq80aaJsE:gRT0~y$zDc',
    port: 5432,
    ssl: {
        rejectUnauthorized: false, // You can set this to true and provide CA cert for better security
        ca: fs.readFileSync('us-west-1-bundle.pem').toString(), // Path to the downloaded RDS root certificate
    },
  });

app.get('/water_wells', async (req: Request, res: Response) => {
    try {
      const query = `
        SELECT water_well.well_id, water_well.well_name, water_well.longitude, water_well.latitude, water_well.depth, water_well.water_present, well_lithology.lithology
        FROM water_well
        JOIN well_lithology ON water_well.well_id = well_lithology.well_id
        WHERE ST_Intersects(
            water_well.location,
            ST_MakeEnvelope(-120.639024, 34.761779, -120.000, 35.56163, 4326)::geography
        );
      `;

    //   const query = `
    //   SELECT water_well.well_id, water_well.well_name, water_well.longitude, water_well.latitude, water_well.depth, water_well.water_present, well_lithology.lithology
    //     FROM water_well
    //     JOIN well_lithology ON water_well.well_id = well_lithology.well_id
    //     WHERE ST_Contains(
    //         ST_MakeEnvelope(-119.253229, 34.425619, -118.378889, 34.047725, 4326),
    //         water_well.location
    //     );
    // `;

      const result = await pool.query(query);
      res.json(result.rows);
    } catch (error) {
      // Check if the error is an instance of Error and has a message property
      if (error instanceof Error) {
        res.status(500).json({ error: error.message });
      } else {
        // If the error is not an instance of Error, respond with a generic error message
        res.status(500).json({ error: 'An unexpected error occurred' });
      }
    }
  });

// Start the server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

