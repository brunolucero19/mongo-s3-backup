import { exec } from 'child_process'
import util from 'util'
import fs from 'fs'
import path from 'path'
import { tmpdir } from 'os'
import {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  type ObjectIdentifier,
} from '@aws-sdk/client-s3'

const execPromise = util.promisify(exec)

function getEnv(name: string, required = true): string {
  const value = process.env[name]
  if (!value && required) {
    console.error(`[BACKUP] Falta la variable de entorno requerida: ${name}`)
    process.exit(1)
  }
  return value ?? ''
}

function buildBackupKey(prefix: string, date: Date): string {
  const iso = date.toISOString().replace('T', '_').slice(0, 16).replace(':', '-')
  return `${prefix}db-backup-${iso}.gz`
}

function buildTempPath(): string {
  return path.join(tmpdir(), `db-backup-${Date.now()}.gz`)
}

async function dumpDatabase(mongoUri: string): Promise<string> {
  const archivePath = buildTempPath()
  const cmd = `mongodump --uri="${mongoUri}" --archive="${archivePath}" --gzip`

  console.log('[BACKUP] Ejecutando mongodump...')
  const { stderr } = await execPromise(cmd)

  if (stderr && !stderr.includes('done dumping')) {
    console.warn('[BACKUP] mongodump stderr:', stderr)
  }

  if (!fs.existsSync(archivePath)) {
    throw new Error(`[BACKUP] El archivo de dump no fue creado: ${archivePath}`)
  }

  console.log(`[BACKUP] Dump creado en: ${archivePath}`)
  return archivePath
}

async function uploadToS3(
  s3: S3Client,
  bucket: string,
  key: string,
  filePath: string,
): Promise<void> {
  const fileStream = fs.createReadStream(filePath)

  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fileStream,
    ContentType: 'application/gzip',
  })

  console.log(`[BACKUP] Subiendo a s3://${bucket}/${key} ...`)
  await s3.send(command)
  console.log('[BACKUP] Upload completado.')
}

async function deleteOldBackups(
  s3: S3Client,
  bucket: string,
  prefix: string,
  retentionDays: number,
): Promise<void> {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - retentionDays)

  console.log(
    `[BACKUP] Buscando backups anteriores a ${cutoff.toISOString().split('T')[0]}...`,
  )

  const toDelete: ObjectIdentifier[] = []
  let continuationToken: string | undefined

  do {
    const listCommand = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    })

    const {
      Contents = [],
      IsTruncated,
      NextContinuationToken,
    } = await s3.send(listCommand)

    for (const obj of Contents) {
      if (!obj.Key || !obj.LastModified) continue
      if (obj.LastModified < cutoff) {
        toDelete.push({ Key: obj.Key })
      }
    }

    continuationToken = IsTruncated ? NextContinuationToken : undefined
  } while (continuationToken)

  if (toDelete.length === 0) {
    console.log('[BACKUP] No hay backups viejos que eliminar.')
    return
  }

  const deleteCommand = new DeleteObjectsCommand({
    Bucket: bucket,
    Delete: { Objects: toDelete },
  })

  await s3.send(deleteCommand)
  console.log(`[BACKUP] Eliminados ${toDelete.length} backup(s) viejos.`)
}

function cleanupTemp(filePath: string): void {
  try {
    if (fs.existsSync(filePath)) {
      fs.rmSync(filePath, { force: true })
      console.log(`[BACKUP] Archivo temporal eliminado: ${filePath}`)
    }
  } catch (err) {
    console.warn('[BACKUP] No se pudo eliminar archivo temporal:', err)
  }
}

async function runBackup(): Promise<void> {
  const mongoUri = getEnv('DB_URI')
  const s3Bucket = getEnv('AWS_S3_BUCKET')
  const awsRegion = getEnv('AWS_REGION')
  const rawPrefix = getEnv('S3_PREFIX', false)
  const retentionDays = parseInt(process.env['RETENTION_DAYS'] ?? '7', 10)

  const prefix = rawPrefix
    ? rawPrefix.endsWith('/')
      ? `${rawPrefix}backups/database/`
      : `${rawPrefix}/backups/database/`
    : 'backups/database/'

  const s3 = new S3Client({ region: awsRegion })
  const backupKey = buildBackupKey(prefix, new Date())
  let archivePath: string | null = null

  try {
    archivePath = await dumpDatabase(mongoUri)
    await uploadToS3(s3, s3Bucket, backupKey, archivePath)
    await deleteOldBackups(s3, s3Bucket, prefix, retentionDays)
    console.log('[BACKUP] Backup completado exitosamente.')
  } finally {
    if (archivePath) cleanupTemp(archivePath)
  }
}

runBackup()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('[BACKUP] Error fatal:', err)
    process.exit(1)
  })
