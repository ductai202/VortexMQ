namespace DotnetBroker.Core.Protocol;

/// <summary>
/// Wire protocol message types.
/// Codes 1–99 are client→server commands.
/// Codes 101+ are server→client responses (R_ prefix).
/// </summary>
public enum MessageType : byte
{
    // Commands
    Echo    = 1,   // Client→Server: echo payload back
    P_Reg   = 2,   // Producer→Admin: register producer (topic:u32 + port:u16)
    C_Reg   = 3,   // Consumer→Admin: register consumer (topic:u32 + port:u16 + group_id:u32 + mode:u8)
    Pcm     = 4,   // Producer→Admin→Consumer: produce-consume message
    C_Rd    = 5,   // Consumer→Admin: consumer is ready to receive (Pull mode)

    // Responses
    R_Echo  = 101, // Server→Client: echo response
    R_P_Reg = 102, // Admin→Producer: producer registration ACK
    R_C_Reg = 103, // Admin→Consumer: consumer registration ACK
    R_Pcm   = 104, // Consumer→Admin: message received ACK
    R_C_Rd  = 105, // Admin→Consumer: ready signal ACK
}
