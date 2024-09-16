/** ******************************************************************************
 *  (c) 2019-2024 Zondax AG
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************* */
import type Transport from '@ledgerhq/hw-transport'
import BaseApp, { BIP32Path, INSGeneric, processErrorResponse, processResponse } from '@zondax/ledger-js'
import { ByteStream } from '@zondax/ledger-js/dist/byteStream'

import { P1_VALUES, PUBKEYLEN } from './consts'
import { Account, AccountType, ResponseAddress, VaultAccount } from './types'
import { MsgSigner, ResponseSign } from './types'

export class SpaceMeshApp extends BaseApp {
    static _INS = {
        GET_VERSION: 0x00 as number,
        GET_ADDR: 0x01 as number,
        SIGN: 0x02 as number,

        GET_ADDR_MULTISIG: 0x03 as number,
        GET_ADDR_VESTING: 0x04 as number,
        GET_ADDR_VAULT: 0x05 as number,
        SIGN_MESSAGE: 0x06 as number,
    }

    static _params = {
        cla: 0x45,
        ins: { ...SpaceMeshApp._INS } as INSGeneric,
        p1Values: { ONLY_RETRIEVE: 0x00 as 0, SHOW_ADDRESS_IN_DEVICE: 0x01 as 1 },
        chunkSize: 250,
        requiredPathLengths: [5],
    }

    constructor(transport: Transport) {
        super(transport, SpaceMeshApp._params)
        if (!this.transport) {
            throw new Error('Transport has not been defined')
        }
    }

    async getAddressAndPubKey(path: string, genesisId: Buffer, showAddrInDevice = false): Promise<ResponseAddress> {
        const p2 = showAddrInDevice ? P1_VALUES.SHOW_ADDRESS_IN_DEVICE : P1_VALUES.ONLY_RETRIEVE
        return await this.getAddressGeneric(path, p2, this.INS.GET_ADDR, genesisId)
    }

    async getAddressMultisig(path: string, internalIndex: number, account: Account, genesisId: Buffer): Promise<ResponseAddress> {
        account.checkSanity(internalIndex)

        const bs = new ByteStream()
        bs.appendBytes(genesisId)
        bs.appendUint8(internalIndex)
        bs.appendBytes(account.serialize())
        const payload = bs.getCompleteBuffer()

        if (account.type !== AccountType.Multisig) {
            throw new Error('Invalid account type for multisig address')
        }
        const ins = this.INS.GET_ADDR_MULTISIG

        return await this.getAddressGeneric(path, 0, ins, payload)
    }

    async getAddressVesting(path: string, internalIndex: number, account: Account, genesisId: Buffer): Promise<ResponseAddress> {
        account.checkSanity(internalIndex)

        const bs = new ByteStream()
        bs.appendBytes(genesisId)
        bs.appendUint8(internalIndex)
        bs.appendBytes(account.serialize())
        const payload = bs.getCompleteBuffer()

        if (account.type !== AccountType.Vesting) {
            throw new Error('Invalid account type for vesting address')
        }

        const ins = this.INS.GET_ADDR_VESTING

        return await this.getAddressGeneric(path, 0, ins, payload)
    }

    async getAddressVault(path: string, internalIndex: number, account: VaultAccount, genesisId: Buffer): Promise<ResponseAddress> {
        account.checkSanity(internalIndex)

        const bs = new ByteStream()
        bs.appendBytes(genesisId)
        bs.appendUint8(internalIndex)
        bs.appendBytes(account.serialize())
        const payload = bs.getCompleteBuffer()

        if (account.type !== AccountType.Vault) {
            throw new Error('Invalid account type for vesting address')
        }
        const ins = this.INS.GET_ADDR_VAULT

        return await this.getAddressGeneric(path, 0, ins, payload)
    }

    private async getAddressGeneric(path: string, p2: number, ins: number, payload: Buffer): Promise<ResponseAddress> {
        const chunks = this.prepareChunks(path, payload)

        try {
            let response
            for (let i = 0; i < chunks.length; i++) {
                response = await this.sendGenericChunk(ins, p2, i + 1, chunks.length, chunks[i])
            }

            if (!response) {
                throw new Error('Failed to receive response from device')
            }

            return {
                pubkey: response.readBytes(PUBKEYLEN),
                address: response.getAvailableBuffer().toString(),
            } as ResponseAddress
        } catch (e) {
            throw processErrorResponse(e)
        }
    }

    async sign(path: BIP32Path, blob: Buffer): Promise<ResponseSign> {
        const chunks = this.prepareChunks(path, blob)
        try {
            let signatureResponse = await this.signSendChunk(this.INS.SIGN, 1, chunks.length, chunks[0])

            for (let i = 1; i < chunks.length; i += 1) {
                signatureResponse = await this.signSendChunk(this.INS.SIGN, 1 + i, chunks.length, chunks[i])
            }
            return {
                signature: signatureResponse.readBytes(signatureResponse.length()),
            }
        } catch (e) {
            throw processErrorResponse(e)
        }
    }

    async signMessage(path: BIP32Path, signMessage: MsgSigner): Promise<ResponseSign> {
        const bs = new ByteStream()
        bs.appendUint16(signMessage.prefix.length)
        bs.appendUint16(signMessage.message.length)
        bs.appendBytes(signMessage.prefix)
        bs.appendUint8(signMessage.domain)
        bs.appendBytes(signMessage.message)

        const payload = bs.getCompleteBuffer()

        const chunks = this.prepareChunks(path, payload)
        try {
            let signatureResponse = await this.signSendChunk(this.INS.SIGN_MESSAGE, 1, chunks.length, chunks[0])

            for (let i = 1; i < chunks.length; i += 1) {
                signatureResponse = await this.signSendChunk(this.INS.SIGN_MESSAGE, 1 + i, chunks.length, chunks[i])
            }
            return {
                signature: signatureResponse.readBytes(signatureResponse.length()),
            }
        } catch (e) {
            throw processErrorResponse(e)
        }
    }
}

