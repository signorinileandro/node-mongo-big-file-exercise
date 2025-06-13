const fs = require("fs");
const readline = require("readline");
const Records = require("./records.model");

const upload = async (req, res) => {
  const { file } = req;

  if (!file) {
    // Considera loguear este caso también, aunque ya devuelves un 400
    console.warn("Intento de subida sin archivo.");
    return res.status(400).json({ message: "No se ha subido ningún archivo." });
  }

  const filePath = file.path;
  const originalFilename = file.originalname;
  console.log(
    `Iniciando procesamiento del archivo: ${originalFilename} (ruta temporal: ${filePath})`
  );

  // Tamaño del lote para inserciones en la base de datos
  const BATCH_SIZE = 1000;
  // Array para almacenar los registros antes de insertarlos en la base de datos
  let recordsBatch = [];
  // Contador de registros procesados
  let processedCount = 0;
  // Contador de líneas procesadas
  let lineCount = 0;
  // Omitir cabecera del archivo CSV
  let isFirstLine = true;

  try {
    const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      lineCount++;
      if (isFirstLine) {
        isFirstLine = false;
        continue;
      }

      // Asumimos que el CSV está separado por comas y tiene el siguiente orden:
      // id,firstname,lastname,email,email2,profession
      const values = line.split(",");

      // Validar que la línea tenga el número esperado de columnas.
      // El modelo Records tiene 6 campos.
      if (values.length === 6) {
        const record = {
          id: parseInt(values[0], 10),
          firstname: values[1],
          lastname: values[2],
          email: values[3],
          email2: values[4],
          profession: values[5],
        };
        recordsBatch.push(record);

        if (recordsBatch.length >= BATCH_SIZE) {
          // En caso de que el tamaño del lote alcance el límite, insertar en la base de datos y resetea el lote
          await Records.insertMany(recordsBatch);
          console.log(
            `Lote de ${
              recordsBatch.length
            } registros insertado. Total procesados hasta ahora: ${
              processedCount + recordsBatch.length
            }`
          );
          processedCount += recordsBatch.length;
          recordsBatch = [];
        }
      } else {
        console.warn(
          `Línea ${lineCount} omitida por formato incorrecto (columnas: ${
            values.length
          }): ${line.substring(0, 100)}...`
        );
      }
    }

    // Insertar los registros restantes en el último lote
    if (recordsBatch.length > 0) {
      await Records.insertMany(recordsBatch);
      console.log(
        `Lote final de ${
          recordsBatch.length
        } registros insertado. Total procesados: ${
          processedCount + recordsBatch.length
        }`
      );
      processedCount += recordsBatch.length;
    }

    // Cerrar readline y file stream
    rl.close();
    fileStream.close();

    console.log(
      `Procesamiento completado para ${originalFilename}. Total de líneas leídas: ${lineCount}. Registros insertados: ${processedCount}.`
    );
    return res.status(200).json({
      message: `Se procesaron ${processedCount} registros exitosamente.`,
    });
  } catch (error) {
    console.error(`Error procesando el archivo ${originalFilename}:`, error);
    return res
      .status(500)
      .json({ message: "Error al procesar el archivo.", error: error.message });
  } finally {
    // Eliminar el archivo al finalizar el proceso
    fs.unlink(filePath, (err) => {
      if (err) {
        console.error("Error al eliminar el archivo temporal:", err);
      } else {
        console.log("Archivo temporal eliminado exitosamente:", filePath);
      }
    });
  }
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).limit(10).lean();

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json(err);
  }
};

module.exports = {
  upload,
  list,
};
